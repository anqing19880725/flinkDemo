package com.atguigu.apitest.source;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest1_Collection {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从集合中读取数据
        DataStream<SensorReading> dataStreamSource = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8)
                , new SensorReading("sensor_6", 1547718201L, 15.4)
                , new SensorReading("sensor_7", 1547718202L, 6.7)
                , new SensorReading("sensor_10", 1547718205L, 38.1)
                )
        );

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);
        dataStreamSource.print("data");
        integerDataStreamSource.print("int");

        env.execute();
    }

}
