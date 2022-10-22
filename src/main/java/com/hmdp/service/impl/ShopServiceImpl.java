package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;


    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);

        //解决缓存击穿问题  ----   互斥锁
        Shop shop = queryWithMutex(id);
        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);
    }

    //解决缓存击穿问题  ----   互斥锁
    public Shop queryWithMutex(Long id) {
        //1、从redis查询商铺缓存
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2、判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3、存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断命中的值是否为空 ---  set(key, "")
        if ("".equals(shopJson)) {
            // 返回错误信息
            return null;
        }
        //4、实现缓存重建
        //4.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2 判断是否获取成功
            if (!isLock) {
                //4.3 失败，休眠重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            //4.4 成功，根据id查询数据库
            shop = this.getById(id);
            if (shop == null) {
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                //5、不存在，返回错误
                return null;
            }
            //6、存在，写入redis   --- 设置超时时间
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //7、释放互斥锁
            unlock(lockKey);
        }
        return shop;
    }

    //缓存穿透  -----   增加缓存保留时间
    public Shop queryWithPassThrough(Long id) {
        //1、从redis查询商铺缓存
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2、判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3、存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断命中的值是否为空 ---  set(key, "")
        if ("".equals(shopJson)) {
            // 返回错误信息
            return null;
        }
        //4、不存在，根据id查询数据库
        Shop shop = this.getById(id);
        if (shop == null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            //5、不存在，返回错误
            return null;
        }
        //6、存在，写入redis后返回   --- 设置超时时间
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    //尝试获取互斥锁 --- redis 中 setnx操作
    private boolean tryLock(String key) {
        Boolean res = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        //**** 防止自动拆箱出现空指针 *****
        return BooleanUtil.isTrue(res);
    }

    //释放锁
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        //1、更新数据库
        this.updateById(shop);
        //2、删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
