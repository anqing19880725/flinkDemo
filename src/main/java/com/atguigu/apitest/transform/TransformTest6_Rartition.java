package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest6_Rartition {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\idea_workspace\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        dataStream.print("input");
//        inputStream.print("input");

        // 1.shuffle
//        DataStream<String> shuffleStream = inputStream.shuffle();
//        shuffleStream.print("shuffle");

        // 2.keyBy
//        dataStream.keyBy("id").print("keyBy");

        // 3.global
        dataStream.global().print("global");

        env.execute();


    }

}
