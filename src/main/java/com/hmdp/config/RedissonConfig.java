package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @BelongsProject: ajie_comment
 * @BelongsPackage: com.hmdp.config
 * @Author: ajie
 * @Date: 2022/10/22 19:48
 * @Description: TODO
 */
@Configuration
public class  RedissonConfig {

    @Bean
    public RedissonClient redissonClient(){
        // 配置
        Config config = new Config();
        //添加redis地址，这里添加的是单点的地址，也可以使用config.useClusterServers()添加集群地址
        config.useSingleServer().setAddress("redis://192.168.175.132:6379");
        // 创建RedissonClient对象
        return Redisson.create(config);
    }
}
