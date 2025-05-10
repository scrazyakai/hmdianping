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
local isOrdered = redis.call('SISMEMBER', orderKey, userId)
if tonumber(isOrdered) == 1 then
    -- 存在，说明重复下单
    return 2
end

-- 2. 判断库存是否充足
local stockStr = redis.call('GET', stockKey)
if not stockStr then
    -- 库存不存在
    return 1
end

local stock = tonumber(stockStr)
if not stock or stock <= 0 then
    -- 库存不足
    return 1
end

-- 3. 扣库存
redis.call('SET', stockKey, tostring(stock - 1))

-- 4. 记录用户下单
redis.call('SADD', orderKey, userId)
--发送消息到消息队列
redis.call('xadd','stream:order','*','userId',userId,'voucherId',voucherId,'id',orderId)
-- 5. 返回成功
return 0