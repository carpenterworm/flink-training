package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class ConnectedStreamsExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> control = env
                .fromElements("DROP", "IGNORE")
                .keyBy(x -> x);

        DataStream<String> streamOfWords = env
                .fromElements("IGNORE", "Apache", "DROP", "Flink", "IGNORE", "DROP", "IGNORE", "DROP", "IGNORE", "DROP")
                .keyBy(x -> x);

        control
                .connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        env.execute();
    }

    public static class ControlFunction extends RichCoFlatMapFunction<String, String, String> implements InputSelectable {
        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration config) {
            blocked = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }

        @Override
        public void flatMap1(String control_value, Collector<String> out) throws Exception {
            blocked.update(Boolean.TRUE);
        }

        @Override
        public void flatMap2(String data_value, Collector<String> out) throws Exception {
            if (blocked.value() == null) {
                out.collect(data_value);
            }
        }

        /**
         * 在这个例子中，selectInput方法检查控制状态是否被设置。如果未被设置，它优先处理控制流（即第一个输入）；
         * 如果已被设置，它优先处理数据流（即第二个输入）。这样，你可以确保在处理数据之前先处理所有的控制信息，从而保证消费的顺序。
         * @return
         */
        @Override
        public InputSelection nextSelection() {
            try {
                if (blocked.value() == null) {
                    return InputSelection.FIRST;
                } else {
                    // 否则，优先处理数据流
                    return InputSelection.SECOND;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

