package com.example.esdemo.dto;

import java.math.BigDecimal;

/**
 * 商品
 */
public class Product {
    /**
     * 主键id
     */
    private Long id;
    /**
     * 商品名
     */
    private String name;
    /**
     * 商品短标题
     */
    private String subTitle;
    /**
     * 商品价格
     */
    private BigDecimal price;
    /**
     * 商品图片
     */
    private String pics;

    public String getPics() {
        return pics;
    }

    public void setPics(String pics) {
        this.pics = pics;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSubTitle() {
        return subTitle;
    }

    public void setSubTitle(String subTitle) {
        this.subTitle = subTitle;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }
}
