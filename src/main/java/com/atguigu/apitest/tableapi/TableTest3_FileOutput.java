package com.atguigu.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

public class TableTest3_FileOutput {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 表的创建:连接外部系统读取数据
        // 2.1 读取文件
        String filePath = "D:\\idea_workspace\\flinkDemo\\src\\main\\resources\\sensor.txt";
        tableEnv.connect(new FileSystem().path(filePath))
//                .withFormat(new OldCsv())
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print();

        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select("id,temperature")
                .filter("id === 'sensor_6'");

        // 聚合统计
        //id.count和temperature.avg是自带的方法应该类似于count(id)和avg(temperature)
        Table aggTable = inputTable.groupBy("id")
                .select("id,id.count as count , temperature.avg as avgTemp");

        // 3.2 SQL
        tableEnv.sqlQuery("select id,temperature from inputTable where id = 'sensor_6' ");
        Table sqlAggTable = tableEnv.sqlQuery("select id,count(id) cnt,avg(temperature) avgTemp from inputTable group by id");

        // 4. 输出到文件
        // 4.1 连接外部文件注册输出表
        String outputPath = "D:\\idea_workspace\\flinkDemo\\src\\main\\resources\\out.txt";
        tableEnv.connect(new FileSystem().path(outputPath))
//                .withFormat(new OldCsv())
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("cnt",DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");
//        aggTable.insertInto("outputTable");// 报错 更新行为是先删后插 文件不支持先删后插

        env.execute();
    }
}
