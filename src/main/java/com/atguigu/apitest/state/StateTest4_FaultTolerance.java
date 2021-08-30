package com.atguigu.apitest.state;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple3;

public class StateTest4_FaultTolerance {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("HDFS的路径"));
        env.setStateBackend(new RocksDBStateBackend(""));


        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print();
        env.execute();
    }
}
