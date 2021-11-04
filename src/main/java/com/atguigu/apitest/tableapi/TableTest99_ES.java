package com.atguigu.apitest.tableapi;


import org.apache.avro.data.Json;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

public class TableTest99_ES {

    public static void main(String[] args) throws Exception {

        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 2. 连接kafka,读取数据
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        // 3. 查询转换
        // 简单转换
        Table sensorTable = tableEnv.from("inputTable");
        Table resultTable = sensorTable.select("id,temperature")
                .filter("id === 'sensor_6'");

        // 聚合统计
        //id.count和temperature.avg是自带的方法应该类似于count(id)和avg(temperature)
        Table aggTable = sensorTable.groupBy("id")
                .select("id,id.count as count , temperature.avg as avgTemp");

        tableEnv.connect(new Elasticsearch()
                .version("6")
                .host("localhost", 9200, "http")
                .index("sensor")
                .documentType("temperature")
        )
                .inUpsertMode()
//                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("count", DataTypes.BIGINT())
                        .field("avgTemp",DataTypes.DOUBLE())
                )
                .createTemporaryTable("esoutputTable");

        aggTable.insertInto("esoutputTable");

        env.execute();
    }
}
