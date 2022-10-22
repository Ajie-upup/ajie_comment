package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;


    @Override
    public Result seckillVoucher(Long voucherId) {
        //1、查询优惠卷信息
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

        //2、判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            //尚未开始
            return Result.fail("秒杀尚未开始");
        }

        //3、判断秒杀是否结束
        if (voucher.getBeginTime().isBefore(LocalDateTime.now())) {
            //已经结束
            return Result.fail("秒杀已经结束");
        }

        //4、判断库存是否充足
        if (voucher.getStock() < 1) {
            //库存不足
            return Result.fail("库存不足！");
        }
        Long userId = UserHolder.getUser().getId();
        /*
        synchronized (userId.toString().intern()) {
            //获取代理对象(事务)
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }
         */
        //创建锁对象
        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        //获取锁
        boolean isLock = lock.tryLock(1200);
        //判断获取锁是否成功
        if (!isLock){
            //获取锁成功，返回错误或者重试
            return Result.fail("不允许重复下单");
        }
        try {
            //获取代理对象(事务)
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }

    }

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        //5、一人一单
        Long userId = UserHolder.getUser().getId();

        //5.1、查询订单
        int count = this.query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        //5.2、判断是否存在
        if (count > 0) {
            //用户已经购买过
            return Result.fail("用户已经购买过一次了！");
        }

        //6、扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                //乐观锁CAS判断
                .gt("stock", 0)
                .update();
        if (!success) {
            //库存不足
            return Result.fail("库存不足！");
        }

        //7、订单不存在，则创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //7.1、订单id
        Long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //7.2、用户id
        voucherOrder.setUserId(userId);
        //7.3、代金卷id
        voucherOrder.setVoucherId(voucherId);

        //新增订单
        this.save(voucherOrder);
        //7、返回订单id
        return Result.ok(orderId);
    }

}
