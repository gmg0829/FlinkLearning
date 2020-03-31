package com.gmg.partition;

import akka.japi.tuple.Tuple3;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author gmg
 * @title: Test
 * @projectName FlinkLearning
 * @description: TODO
 * @date 2020/3/31 16:24
 */
public class Test {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream("127.0.0.1",9000);
        data.setParallelism(4).global().print().setParallelism(2);
        data.setParallelism(4).shuffle().print().setParallelism(2);
        data.setParallelism(4).rebalance().print().setParallelism(2);
        data.setParallelism(4).rescale().print().setParallelism(2);
        data.setParallelism(4).broadcast().print().setParallelism(2);
        data.setParallelism(4).forward().print().setParallelism(2);
        data.setParallelism(2)
                .keyBy(0)
                .print()
                .setParallelism(4);

        data.setParallelism(2)
                // 采用CUSTOM分区策略重分区
                .partitionCustom(new CustomPartitioner(),0)
                .print()
                .setParallelism(4);

    }

    // 自定义分区器，将不同的Key(用户ID)分到指定的分区
    // key: 根据key的值来分区
    // numPartitions: 下游算子并行度
    static class CustomPartitioner implements Partitioner<String> {
        @Override
        public int partition(String key, int numPartitions) {
            switch (key){
                case "user_1":
                    return 0;
                case "user_2":
                    return 1;
                case "user_3":
                    return 2;
                default:
                    return 3;
            }
        }
    }

}
