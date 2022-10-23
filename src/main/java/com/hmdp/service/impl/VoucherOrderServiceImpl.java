package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    //初始化lua脚本
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    /**
     * 秒杀优惠卷：
     * 1、扣减优惠卷的库存
     * 2、将用户抢购优惠卷的信息写入订单，完成订单创建
     * 3、存在超卖现象，实现一人一单
     * 4、基于阻塞队列实现秒杀异步下单
     *
     * @param voucherId
     * @return
     */
    //线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //创建阻塞队列，存放订单
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    //获取代理对象的成员变量
    IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //1、执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        int r = result.intValue();
        //2、判断结果是否为0
        if (r != 0) {
            //2.1、不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 2.2.为0 ，有购买资格，把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.3.订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 2.4.用户id
        voucherOrder.setUserId(userId);
        // 2.5.代金券id
        voucherOrder.setVoucherId(voucherId);
        // 2.6.放入阻塞队列
        orderTasks.add(voucherOrder);
        // 3.父线程获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 4.返回订单id
        return Result.ok(orderId);
    }

    //实现线程任务
    public class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }

    //类初始化时执行线程池
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    //处理订单
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //获取用户id
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断获取锁是否成功
        if (!isLock) {
            //获取锁成功，返回错误或者重试
            log.error("不能重复下单");
            return;
        }
        try {
//      获取代理对象(事务) ---- 子线程无法从ThreadLocal中获取代理对象,需要在父线程中提前获取代理对象
//      IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }


    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //5、一人一单
        Long userId = voucherOrder.getUserId();

        //5.1、查询订单
        int count = this.query().eq("user_id", userId).eq("voucher_id", voucherOrder).count();
        //5.2、判断是否存在
        if (count > 0) {
            //用户已经购买过
            log.error("用户已经购买过一次了！");
            return;
        }

        //6、扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder)
                //乐观锁CAS判断
                .gt("stock", 0)
                .update();
        if (!success) {
            //库存不足
            log.error("库存不足！");
            return;
        }
        //新增订单
        this.save(voucherOrder);
        //7、返回订单id  ---  异步执行不需要返回
//        return Result.ok(voucherOrder.getId());
    }

    /*基本实现秒卷的抢购功能
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
        //基于redisson实现的锁 (*)
        //创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断获取锁是否成功
        if (!isLock) {
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
        //synchronized实现锁
//        synchronized (userId.toString().intern()) {
//            //获取代理对象(事务)
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }

        //基于setnx实现的自定义锁
//        //创建锁对象
//        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//
//        //获取锁
//        boolean isLock = lock.tryLock(1200);
//        //判断获取锁是否成功
//        if (!isLock) {
//            //获取锁成功，返回错误或者重试
//            return Result.fail("不允许重复下单");
//        }
//        try {
//            //获取代理对象(事务)
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
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
     */
}
