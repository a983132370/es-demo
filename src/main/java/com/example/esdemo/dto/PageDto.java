package com.example.esdemo.dto;

import java.io.Serializable;
import java.util.List;

/**
 * 分页对象
 */
public class PageDto<T> implements Serializable {
    static final long serialVersionUID = 1L;

    private int page;
    private int pageSize;
    private int allRecord;
    private int totalPage;
    List<T> list;

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getAllRecord() {
        return allRecord;
    }

    public void setAllRecord(int allRecord) {
        this.allRecord = allRecord;
    }

    public int getTotalPage() {
        return totalPage;
    }

    public void setTotalPage(int totalPage) {
        this.totalPage = totalPage;
    }

    public List<T> getList() {
        return list;
    }

    public void setList(List<T> list) {
        this.list = list;
    }
}
