package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IBlogService extends IService<Blog> {
    /**
     * 查询热门笔记
     * @param current
     * @return
     */
    Result queryHotBlog(Integer current);

    /**
     * 查询笔记
     * @return
     */
    Result queryBlog(Long id);

    /**
     * 点赞博客
     * @param id
     * @return
     */
    Result likeBlog(Long id);

    Result queryBlogLikes(Long id);

    Result saveBlog(Blog blog);

    Result queryBlogOfFollow(Long max, Integer offset);
}
