package com.gmg.stream;

import com.gmg.bean.Order;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author gmg
 * @title: Main
 * @projectName FlinkLearning
 * @description: TODO
 * @date 2020/3/1 10:39
 */
public class Main{
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper,apple", 4),
                new Order(3L, "rubber", 2)));


        orderA.map(new MapFunction<Order, Integer>() {
            @Override
            public Integer map(Order order) throws Exception {
                return order.amount;
            }
        });

        orderA.flatMap(new FlatMapFunction<Order, String>() {
            @Override
            public void flatMap(Order order, Collector<String> collector) throws Exception {
                for (String v:order.getProduct().split(",")){
                    collector.collect(v);
                }
            }
        });

        orderA.filter(new FilterFunction<Order>() {
            @Override
            public boolean filter(Order order) throws Exception {
                return order.getUser()==1L;
            }
        });

        orderA.keyBy("user").reduce(new ReduceFunction<Order>() {
            @Override
            public Order reduce(Order order1, Order order2) throws Exception {
                return new Order(order1.getUser(),
                        String.join(",",order1.getProduct(),order2.getProduct())
                ,order1.getAmount()+order2.getAmount());
            }
        });

        orderA.keyBy("user").sum("amount");

        env.execute("Stream");


    }


}
