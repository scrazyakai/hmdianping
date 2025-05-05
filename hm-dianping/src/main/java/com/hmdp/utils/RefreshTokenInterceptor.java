package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.aopalliance.intercept.Interceptor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;

public class RefreshTokenInterceptor implements HandlerInterceptor {
    private final StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
        // 验证构造函数注入
        System.out.println("RefreshTokenInterceptor: stringRedisTemplate = " + stringRedisTemplate);
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 1. 获取请求头中的 token
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)) {
            // token 缺失，放行
            return true;
        }

        // 2. 拼接 Redis 中的 key
        String key = LOGIN_USER_KEY + token;

        // 3. 检查 stringRedisTemplate 是否为 null
        if (stringRedisTemplate == null) {
            System.err.println("stringRedisTemplate is null in RefreshTokenInterceptor");
            response.setStatus(500); // Internal Server Error
            return true;
        }

        // 4. 查询 Redis 中的用户信息
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(key);

        // 5. 判断是否存在用户信息
        if (userMap.isEmpty()) {
            // 用户信息不存在，拦截
            return true;
        }

        // 6. 将查询的 Hash 数据转为 UserDTO 对象
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);

        // 7. 保存用户信息到 ThreadLocal
        UserHolder.saveUser(userDTO);

        // 8. 刷新 token 有效期
        stringRedisTemplate.expire(key, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);

        // 9. 放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
