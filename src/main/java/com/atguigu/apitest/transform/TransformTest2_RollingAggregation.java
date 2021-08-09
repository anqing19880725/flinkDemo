package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest2_RollingAggregation {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\idea_workspace\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // 转化成SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String value) throws Exception {
//                String[] fields = value.split(",");
//                return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
//            }
//        });

        DataStream<SensorReading> dataStream = inputStream.map( line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        } );

        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

//        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(data -> data.getId());
//        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(SensorReading::getId);

        // 滚动聚合,取当前最大的温度值
//        DataStream<SensorReading> resultStream = keyedStream.max("temprature");
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temprature");
        resultStream.print();

        env.execute();
    }
}