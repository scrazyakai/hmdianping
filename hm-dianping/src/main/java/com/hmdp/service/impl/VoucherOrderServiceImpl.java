package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import com.sun.istack.internal.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    /**
     * 自己注入自己为了获取代理对象 @Lazy 延迟注入 避免形成循环依赖
     */
    @Resource
    @Lazy
    private IVoucherOrderService voucherOrderService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static{
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
        log.info("Lua脚本加载完成: {}", SECKILL_SCRIPT.getScriptAsString());
    }
//    private BlockingQueue<VoucherOrder> orderTasks = new LinkedBlockingQueue<>(1024 * 1024);
    private static final String queueName = "stream.order";
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    //项目启动后就执行这个线程
    @PostConstruct
    public void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    //消息队列
    class VoucherOrderHandler implements Runnable{
        private static final long REDIS_CONNECTION_RETRY_DELAY_MS = 5000; // 5秒
        private static final long PENDING_LIST_ERROR_DELAY_MS = 1000; // 1秒

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()){ // 检查中断状态
                try {
                    //获取消息队列中的订单消息 XREAD GROUP g1 c1 COUNT 1 BLOCK 2000 STREAM queueNAME
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    //判断消息是否获取成功
                    if(list == null || list.isEmpty() ){
                        // 无新消息，继续下一次轮询
                        continue;
                    }
                    //解析订单中的消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    //键值对对象
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //获取成功可以下单
                    handlerVoucherOrder(voucherOrder);
                    //ACK 确认 XACK stream.order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId() );
                } catch (org.springframework.data.redis.RedisSystemException e) {
                    log.error("订单处理时发生Redis系统异常，将稍后重试: {}", e.getMessage());
                    try {
                        Thread.sleep(REDIS_CONNECTION_RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        log.warn("订单处理线程在休眠时被中断");
                        Thread.currentThread().interrupt(); // 重新设置中断状态
                        break; // 退出循环
                    }
                    // 发生连接相关的系统异常后，可以尝试处理pending list，
                    // 但如果handlerPendingList也因连接问题失败，它内部也应该有合理的退避策略
                    handlerPendingList();
                } catch (Exception e) {
                    log.error("订单处理时发生未知异常",e);
                    // 对于其他类型的异常，也考虑是否需要不同的处理逻辑或延迟
                    try {
                        Thread.sleep(PENDING_LIST_ERROR_DELAY_MS); // 对于其他异常，也稍作等待
                    } catch (InterruptedException ie) {
                        log.warn("订单处理线程在休眠时被中断");
                        Thread.currentThread().interrupt();
                        break;
                    }
                    handlerPendingList(); // 尝试处理pending list
                }
            }
            log.info("VoucherOrderHandler 线程已停止。");
        }

        private void handlerPendingList() {
            while (!Thread.currentThread().isInterrupted()){ // 检查中断状态
                try {
                    //获取消息队列中的订单消息 XREAD GROUP g1 c1 COUNT 1 STREAM queueNAME 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1), // 不阻塞，立即返回
                            StreamOffset.create(queueName, ReadOffset.from("0")) // 从pending-list的开头读取
                    );

                    //判断消息是否获取成功
                    if(list == null || list.isEmpty() ){
                        //pendingList中没有消息或处理完毕
                        log.info("Pending-list中没有更多消息需要处理。");
                        break; // 退出pending-list处理
                    }
                    //解析订单中的消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    //键值对对象
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //获取成功可以下单
                    handlerVoucherOrder(voucherOrder);
                    //ACK 确认 XACK stream.order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId() );
                } catch (org.springframework.data.redis.RedisSystemException e) {
                    log.error("处理pending-list订单时发生Redis系统异常，将稍后重试: {}", e.getMessage());
                    try {
                        Thread.sleep(REDIS_CONNECTION_RETRY_DELAY_MS); // Redis连接问题，较长等待
                    } catch (InterruptedException interruptedException) {
                        log.warn("Pending-list处理线程在休眠时被中断");
                        Thread.currentThread().interrupt(); // 重新设置中断状态
                        break; // 退出循环
                    }
                }
                catch (Exception e) {
                    log.error("处理pending-list订单时发生未知异常",e);
                    try {
                        Thread.sleep(PENDING_LIST_ERROR_DELAY_MS); // 其他异常，较短等待
                    } catch (InterruptedException interruptedException) {
                        log.warn("Pending-list处理线程在休眠时被中断");
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            log.info("handlerPendingList 处理循环结束。");
        }
    }
//    @PostConstruct
//    public void init(){
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
//    }
//    class VoucherOrderHandler implements Runnable{
//        @Override
//        public void run() {
//            while (true){
//                try {
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    handlerVoucherOrder(voucherOrder);
//                } catch (InterruptedException e) {
//                   log.error("订单异常",e);
//                }
//            }
//        }
//    }

    private IVoucherOrderService proxy;
    private void handlerVoucherOrder(VoucherOrder voucherOrder) {
        //获取用户
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        RLock lock = redissonClient.getLock(RedisConstants.LOCK_ORDER_KEY + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        if(!isLock){
            log.error("一人仅允许下一单");
            return; // 获取锁失败要及时返回
        }

        try {
            // 检查订单是否已经存在
            int count = query()
                    .eq("user_id", userId)
                    .eq("voucher_id", voucherOrder.getVoucherId())
                    .count();

            if (count > 0) {
                return;
            }

            // 直接在数据库中扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherOrder.getVoucherId())
                    .gt("stock", 0) // 确保库存充足
                    .update();

            if (!success) {
                log.error("数据库扣减库存失败");
                return;
            }

            // 创建订单记录
            boolean saved = save(voucherOrder);
            log.info("异步订单处理完成: orderId={}, 保存结果={}", voucherOrder.getId(), saved);

        } catch (Exception e) {
            log.error("处理订单异常", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 秒杀优惠券
     *
     * @param voucherId
     * @return {@link Result}
     */
    //基于消息队列实现的秒杀业务
    @Override
    public Result seckillVoucher(Long voucherId){
        //获取用户id
        Long userId = UserHolder.getUser().getId();
        //获取订单id
        long orderId = redisIdWorker.nextId("order");

        // 2. 检查并确保Redis中有库存
        String stockKey = RedisConstants.SECKILL_STOCK_KEY + voucherId;
        String stockStr = stringRedisTemplate.opsForValue().get(stockKey);

        if (stockStr == null) {
            // 如果Redis中没有库存信息，从数据库加载
            SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
            if (voucher == null || voucher.getStock() <= 0) {
                return Result.fail("优惠券不存在或库存不足");
            }

            // 将库存保存到Redis
            stringRedisTemplate.opsForValue().set(stockKey, voucher.getStock().toString());
        }

        // 6. 使用Lua脚本保证操作的原子性
        try {
            // 执行Lua脚本
            Long result = stringRedisTemplate.execute(
                    SECKILL_SCRIPT,
                    Collections.emptyList(),  // KEYS列表为空
                    voucherId.toString(), userId.toString() ,String.valueOf(orderId) // ARGV参数列表
            );

            // 判断结果
            if (result == null) {
                log.error("Lua脚本执行失败，返回null");
                return Result.fail("下单失败");
            }

            int resultCode = result.intValue();
            if (resultCode != 0) {
                if (resultCode == 1) {
                    return Result.fail("库存不足");
                } else if (resultCode == 2) {
                    return Result.fail("不能重复下单");
                } else {
                    return Result.fail("下单失败");
                }
            }

        } catch (Exception e) {
            log.error("执行Lua脚本异常", e);
            return Result.fail("系统异常，请重试");
        }
        // 9. 返回订单ID
        return Result.ok(orderId);
    }
        //基于阻塞队列实现的秒杀业务
//    @Override
//    public Result seckillVoucher(Long voucherId){
//        // 使用Lua脚本实现秒杀，确保原子性操作
//
//        // 1. 获取用户ID
//        Long userId = UserHolder.getUser().getId();
//        log.info("用户{}开始秒杀优惠券{}", userId, voucherId);
//
//        // 2. 检查并确保Redis中有库存
//        String stockKey = RedisConstants.SECKILL_STOCK_KEY + voucherId;
//        String stockStr = stringRedisTemplate.opsForValue().get(stockKey);
//
//        if (stockStr == null) {
//            // 如果Redis中没有库存信息，从数据库加载
//            SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//            if (voucher == null || voucher.getStock() <= 0) {
//                return Result.fail("优惠券不存在或库存不足");
//            }
//
//            // 将库存保存到Redis
//            stringRedisTemplate.opsForValue().set(stockKey, voucher.getStock().toString());
//            log.info("从数据库加载库存: {}", voucher.getStock());
//        }
//
//        // 6. 使用Lua脚本保证操作的原子性
//        try {
//            // 执行Lua脚本
//            Long result = stringRedisTemplate.execute(
//                    SECKILL_SCRIPT,
//                    Collections.emptyList(),  // KEYS列表为空
//                    voucherId.toString(), userId.toString()  // ARGV参数列表
//            );
//
//            // 判断结果
//            if (result == null) {
//                log.error("Lua脚本执行失败，返回null");
//                return Result.fail("下单失败");
//            }
//
//            int resultCode = result.intValue();
//            if (resultCode != 0) {
//                if (resultCode == 1) {
//                    log.info("库存不足");
//                    return Result.fail("库存不足");
//                } else if (resultCode == 2) {
//                    log.info("用户{}重复下单", userId);
//                    return Result.fail("不能重复下单");
//                } else {
//                    log.error("Lua脚本返回未知错误码: {}", resultCode);
//                    return Result.fail("下单失败");
//                }
//            }
//
//            log.info("Lua脚本执行成功，用户{}秒杀优惠券{}成功", userId, voucherId);
//
//        } catch (Exception e) {
//            log.error("执行Lua脚本异常", e);
//            return Result.fail("系统异常，请重试");
//        }
//
//        // 7. 创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherId);
//
//        // 8. 将订单信息放入队列
//        orderTasks.add(voucherOrder);
//        log.info("订单已加入队列，订单号: {}", orderId);
//
//        // 9. 返回订单ID
//        return Result.ok(orderId);
//    }
    
//    /**
//     * 当Lua脚本失败时的备选方案
//     */
//    private Result fallbackToDirectOrder(Long voucherId, Long userId) {
//        log.info("使用备选方案下单");
//
//        // 定义Redis键
//        String stockKey = RedisConstants.SECKILL_STOCK_KEY + voucherId;
//        String orderKey = "seckill:order:" + voucherId;
//
//        // 1. 判断是否重复下单
//        Boolean ordered = stringRedisTemplate.opsForSet().isMember(orderKey, userId.toString());
//        if (Boolean.TRUE.equals(ordered)) {
//            return Result.fail("不能重复下单");
//        }
//
//        // 2. 判断库存是否充足
//        String stockStr = stringRedisTemplate.opsForValue().get(stockKey);
//        int stock = 0;
//        try {
//            stock = Integer.parseInt(stockStr);
//        } catch (Exception e) {
//            // 恢复库存
//            SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//            if (voucher != null && voucher.getStock() > 0) {
//                stock = voucher.getStock();
//                stringRedisTemplate.opsForValue().set(stockKey, stock + "");
//            }
//        }
//
//        if (stock <= 0) {
//            return Result.fail("库存不足");
//        }
//
//        // 3. 扣减库存
//        stringRedisTemplate.opsForValue().decrement(stockKey);
//
//        // 4. 记录用户下单
//        stringRedisTemplate.opsForSet().add(orderKey, userId.toString());
//
//        // 5. 创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherId);
//
//        // 6. 添加到异步队列
//        orderTasks.add(voucherOrder);
//
//        return Result.ok(orderId);
//    }
    @Transactional(rollbackFor = Exception.class)
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //是否下单
        Long userId = UserHolder.getUser().getId();
        int count = query().eq("user_id",userId).eq("voucher_id",voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("禁止重复购买");
            return;
        }
        //扣减库存
        boolean isSuccess = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id",voucherOrder.getVoucherId()).update();
        if (!isSuccess) {
            //库存不足
            log.error("库存不足");
            return;
        }
        //创建订单
        this.save(voucherOrder);
    }
}