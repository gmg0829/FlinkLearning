package com.gmg.window;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author gmg
 * @title: Main
 * @projectName FlinkLearning
 * @description: TODO
 * @date 2020/3/1 14:11
 */
public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream("127.0.0.1",9000);

        //基于滚动时间窗口
       /* data.flatMap(new LineSplitter())
                .keyBy(1)
                .timeWindow(Time.seconds(30))
                .sum(0)
                .print();*/

       /* data.flatMap(new LineSplitter())
                .keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .sum(0)
                .print();*/



        //基于滑动时间窗口
/*        data.flatMap(new LineSplitter())
                .keyBy(1)
                .timeWindow(Time.seconds(60), Time.seconds(30))
                .sum(0)
                .print();*/

        /*data.flatMap(new LineSplitter())
                .keyBy(1)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(0)
                .print();*/


        //基于事件数量窗口
/*        data.flatMap(new LineSplitter())
                .keyBy(1)
                .countWindow(3)
                .sum(0)
                .print();*/


        //基于事件数量滑动窗口
/*        data.flatMap(new LineSplitter())
                .keyBy(1)
                .countWindow(4, 3)
                .sum(0)
                .print();*/


        //基于会话时间窗口
       /* data.flatMap(new LineSplitter())
                .keyBy(1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(0)
                .print();*/


        data.flatMap(new LineSplitter())
                .timeWindowAll(Time.seconds(10)).apply(new AllWindowFunction<Tuple2<Long,String>, Object, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Tuple2<Long, String>> iterable, Collector<Object> collector) throws Exception {
                collector.collect(iterable);
            }
        }).print();

        env.execute("zhisheng —— flink window example");
    }
}
