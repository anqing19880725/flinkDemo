package com.atguigu.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class streamingWordCount {
    public static void main(String[] args) throws Exception {

        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.disableOperatorChaining();

//        String inputPath = "D:\\idea_workspace\\FlinkTutorial\\src\\main\\resources\\hello.txt";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 用parametertool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        DataStream<String> inputDataStream = env.socketTextStream(host,port);

        // 基于数据流进行转化计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new wordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1).setParallelism(2).slotSharingGroup("red").startNewChain();
        resultStream.print();

        // 执行任务
        env.execute();
    }
}
