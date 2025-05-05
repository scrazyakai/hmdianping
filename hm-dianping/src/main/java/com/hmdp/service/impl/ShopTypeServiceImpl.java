package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOPTYPE_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private IShopTypeService typeService;
    @Override
    public Result queryList() {
        String shopTypeJSON = stringRedisTemplate.opsForValue().get(CACHE_SHOPTYPE_KEY);
        if(StrUtil.isNotBlank(shopTypeJSON)){
            List<ShopType> shopTypes = JSONUtil.toList(shopTypeJSON, ShopType.class);
            return Result.ok(shopTypes);
        }
        List<ShopType> typeList = typeService.query().orderByAsc("sort").list();
        if(typeList == null){
            Result.fail("分类不存在");
        }
        stringRedisTemplate.opsForValue().set(CACHE_SHOPTYPE_KEY, JSONUtil.toJsonStr(typeList));
        return Result.ok(typeList);
    }
}
