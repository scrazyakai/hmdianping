package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {
    /**
     * 秒杀优惠券
     *
     * @param voucherId 券id
     * @return {@link Result}
     */
    Result seckillVoucher(Long voucherId);

    /**
     * 得到结果
     *
     * @param voucherOrder 券id
     * @return {@link Result}
     */
    void createVoucherOrder(VoucherOrder voucherOrder);
}
