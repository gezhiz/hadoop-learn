package com.worthto.niuniu.topn;

import com.alibaba.fastjson.JSON;

/**
 * 逆序排序
 * @author gezz
 * @description todo
 * @date 2019/9/27.
 */
public class PageCount implements Comparable<PageCount> {
    private String page;
    private Long count;

    public PageCount(String page, Long count) {
        this.page = page;
        this.count = count;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public int compareTo(PageCount o) {
        return count - o.getCount() == 0 ? o.getPage().compareTo(this.page) : (int) (o.getCount() - this.count);
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
