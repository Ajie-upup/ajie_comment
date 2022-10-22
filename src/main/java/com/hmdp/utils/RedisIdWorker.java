package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * redis自增实现全局唯一ID
 *
 * @BelongsProject: ajie_comment
 * @BelongsPackage: com.hmdp.utils
 * @Author: ajie
 * @Date: 2022/10/22 14:51
 * @Description: TODO
 */
@Component
public class RedisIdWorker {
    /**
     * 开始时间戳
     */
    private static final long BEGIN_TIMESTAMP = 1640995200L;
    /**
     * 序列号位数
     */
    private static final long COUNT_BITS = 32L;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public Long nextId(String keyPrefix) {
        //1、生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;

        //2、生成序列号
        //2.1、获取当前日期，精确到天
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //2.2、实现自增长
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);

        //3、拼接并返回
        return timestamp << COUNT_BITS | count;
    }
    /*
    public static void main(String[] args) {
        LocalDateTime time = LocalDateTime.of(2022, 1, 1, 0, 0, 0);
        long second = time.toEpochSecond(ZoneOffset.UTC);
        System.out.println(second);
    }
     */

}
