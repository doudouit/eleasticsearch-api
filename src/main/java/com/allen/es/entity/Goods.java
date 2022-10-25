package com.allen.es.entity;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * @author 窦建新
 * @version V1.0
 * @project SpringBoot-ElasticSearch-Pro
 * @date Created in 2022/10/25  23:20
 * @description
 **/
@Data
public class Goods implements Serializable {
    /**
     * id
     */
    private String id;
    /**
     * 名字
     */
    private String name;
    /**
     * 价格
     */
    private BigDecimal price;
    /**
     * 描述
     */
    private String description;
    /**
     * 创建日期
     */
    private String create_date;
}