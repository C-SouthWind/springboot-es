package com.chj.model;

import java.io.Serializable;

/**
 * @Description:
 * @Author: chj
 * @Date: 2020/3/27
 */
public class Book implements Serializable {
    private Integer id;

    /**
     * 书名
     */
    private String bookName;

    /**
    * 书价格
    */
    private Double bookPrice;

    /**
     * 书名
     */
    private String bookDetail;

    private static final long serialVersionUID = 1L;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getBookName() {
        return bookName;
    }

    public void setBookName(String bookName) {
        this.bookName = bookName == null ? null : bookName.trim();
    }

    public Double getBookPrice() {
        return bookPrice;
    }

    public void setBookPrice(Double bookPrice) {
        this.bookPrice = bookPrice;
    }

    public String getBookDetail() {
        return bookDetail;
    }

    public void setBookDetail(String bookDetail) {
        this.bookDetail = bookDetail == null ? null : bookDetail.trim();
    }
}