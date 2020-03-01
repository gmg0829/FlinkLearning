package com.gmg.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author gmg
 * @title: Order
 * @projectName FlinkLearning
 * @description: TODO
 * @date 2020/3/1 11:28
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public  class Order {

    public Long user;

    public String product;

    public int amount;
}