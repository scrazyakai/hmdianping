-- 秒杀脚本
-- 参数: 1.优惠券ID 2.用户ID
-- 返回: 0:成功 1:库存不足 2:重复下单

-- 接收参数
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]
-- 定义键名
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
-- 1. 判断用户是否重复下单
if(tonumber(redis.call('get',stockKey)) <= 0) then
    return 1
end
if(redis.call('sismember',orderKey,userId) == 1) then
    return 2
end
--扣库存
redis.call('incrby',stockKey,-1)
-- 4. 记录用户下单
redis.call('SADD', orderKey, userId)
--发送消息到消息队列
redis.call('xadd','stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId)
-- 5. 返回成功
return 0