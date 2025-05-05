package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //设置逻辑
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData), time, unit);
    }
    public <ID,R> R queryWithPassThough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFunction,Long time, TimeUnit unit){
        String key = keyPrefix + id;
        //从Redis查询商铺缓存
        String JSON = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(JSON)){
            //存在直接返回
             return JSONUtil.toBean(JSON,type);
        }
        //判断命中值是否为空
        if(JSON != null){
            //返回错误信息
            return null;
        }
        R r = dbFunction.apply(id);
        if(r == null){
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //存在写入缓存
        this.set(key,r,time,unit);
        return r;
    }
    //锁
    public boolean  tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    //开锁
    public void unlock(String key){
        stringRedisTemplate.delete(key);
    }
    //线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public <R,ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFunction,Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String shopJSON = stringRedisTemplate.opsForValue().get(key);
        // 1.判断是否存在
        if(StrUtil.isBlank(shopJSON)){
            return null;
        }
        // 2.命中，将JSON反序列化为对象
        RedisData redisDate = JSONUtil.toBean(shopJSON, RedisData.class);
        R r = JSONUtil.toBean(JSONUtil.toJsonStr(redisDate.getData()), type);
        LocalDateTime expireTime = redisDate.getExpireTime();
        
        // 3.判断是否过期（注意判空和逻辑）
        if(expireTime != null && LocalDateTime.now().isBefore(expireTime)){
            // 未过期，直接返回
            return r;
        }
        
        // 4.已过期，需要缓存重建
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        try {
            if(isLock){
                // 获取锁成功，开启独立线程实现缓存重建
                CACHE_REBUILD_EXECUTOR.submit(()->{
                    try {
                        // 查询数据库
                        R r1 = dbFunction.apply(id);
                        // 写入Redis
                        this.setWithLogicalExpire(key,r1,time,unit);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        unlock(lockKey);
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        // 返回过期的数据
        return r;
    }
}