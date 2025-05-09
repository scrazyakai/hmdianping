package com.hmdp.service.impl;

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
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    
    @PostConstruct
    public void checkRedisKeys() {
        // 启动时检查一些关键的Redis键
        try {
            String stockKey = RedisConstants.SECKILL_STOCK_KEY + "10"; // 检查ID为10的库存键
            String stock = stringRedisTemplate.opsForValue().get(stockKey);
            log.info("系统启动时检查: 优惠券10的库存为 {}", stock);
        } catch (Exception e) {
            log.error("Redis连接检查失败", e);
        }
    }
    private BlockingQueue<VoucherOrder> orderTasks = new LinkedBlockingQueue<>(1024 * 1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    //项目启动后就执行这个线程
    @PostConstruct
    public void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while (true){
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    handlerVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                   log.error("订单异常",e);
                }
            }
        }
    }
    private IVoucherOrderService proxy;
    private void handlerVoucherOrder(VoucherOrder voucherOrder) {
        //获取用户
        Long userId = voucherOrder.getUserId();
        log.info("开始处理异步订单: userId={}, voucherId={}", userId, voucherOrder.getVoucherId());
        
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
                log.error("用户 {} 已购买过优惠券 {}", userId, voucherOrder.getVoucherId());
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
     * @param voucherId 券id
     * @return {@link Result}
     */

    @Override
    public Result seckillVoucher(Long voucherId){
        // 使用Lua脚本实现秒杀，确保原子性操作
        
        // 1. 获取用户ID
        Long userId = UserHolder.getUser().getId();
        log.info("用户{}开始秒杀优惠券{}", userId, voucherId);
        
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
            log.info("从数据库加载库存: {}", voucher.getStock());
        }
        
        // 6. 使用Lua脚本保证操作的原子性
        try {
            // 执行Lua脚本
            Long result = stringRedisTemplate.execute(
                    SECKILL_SCRIPT,
                    Collections.emptyList(),  // KEYS列表为空
                    voucherId.toString(), userId.toString()  // ARGV参数列表
            );
            
            // 判断结果
            if (result == null) {
                log.error("Lua脚本执行失败，返回null");
                return Result.fail("下单失败");
            }
            
            int resultCode = result.intValue();
            if (resultCode != 0) {
                if (resultCode == 1) {
                    log.info("库存不足");
                    return Result.fail("库存不足");
                } else if (resultCode == 2) {
                    log.info("用户{}重复下单", userId);
                    return Result.fail("不能重复下单");
                } else {
                    log.error("Lua脚本返回未知错误码: {}", resultCode);
                    return Result.fail("下单失败");
                }
            }
            
            log.info("Lua脚本执行成功，用户{}秒杀优惠券{}成功", userId, voucherId);
            
        } catch (Exception e) {
            log.error("执行Lua脚本异常", e);
            return Result.fail("系统异常，请重试");
        }
        
        // 7. 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        
        // 8. 将订单信息放入队列
        orderTasks.add(voucherOrder);
        log.info("订单已加入队列，订单号: {}", orderId);
        
        // 9. 返回订单ID
        return Result.ok(orderId);
    }
    
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