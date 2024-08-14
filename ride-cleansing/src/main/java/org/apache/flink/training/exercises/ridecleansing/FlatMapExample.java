package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapExample {
    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 构建数据源，这里使用了一个简单的字符串集合
        DataStream<String> text = env.fromElements("Apache Flink", "DataStream API");

        // 使用 flatMap 转换操作将每个字符串拆分为单个字符
        DataStream<String> flatMapResult = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (char c : value.toCharArray()) {
                    out.collect(Character.toString(c));
                }
            }
        });

        // 输出结果到控制台
        flatMapResult.print();

        // 执行程序
        env.execute("Flink FlatMap Example");
    }
}

