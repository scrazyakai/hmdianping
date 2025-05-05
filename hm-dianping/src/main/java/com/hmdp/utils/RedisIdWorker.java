package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class RedisIdWorker {
    @Resource
   private StringRedisTemplate stringRedisTemplate;
    //开始时间戳
    private static final long BEGIN_TIME = 1735689600L;
    //序列号位数
    private static final int COUNT_BIT = 32;
    public long nextId(String keyPrefix){
        //1.生成时间戳
        LocalDateTime localDateTime = LocalDateTime.now();
        long seconds = localDateTime.toEpochSecond(ZoneOffset.UTC);
        long timestamp = seconds - BEGIN_TIME;
        //2.生成序列号
        // 获取当天日期
        String date = localDateTime.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //自增长
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);

        //3.拼接并返回

        return timestamp << COUNT_BIT | count;
    }
}
