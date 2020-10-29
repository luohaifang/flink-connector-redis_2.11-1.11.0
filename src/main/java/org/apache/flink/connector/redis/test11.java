package org.apache.flink.connector.redis;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class test11 {
    public static final String REDIS_TABLE_DIM_DDL1 = "" +
            "CREATE TABLE redis_dim1 (\n" +
            "key String,\n" +
            "res String\n" +
            ") WITH (\n" +
            "  'connector.type' = 'redis',  \n" +
            "  'redis.ip' = 'h:p,h:p,h:p',  \n" +
            "  'database.num' = '0', \n"+//不起作用，不知道是不是只有一个数据库的原因
            "  'operate.tpye' = 'string', \n" +
            "  'lookup.cache.max-rows' = '3', \n" +
            "  'lookup.cache.ttl' = '50', \n" +
            "  'redis.version' = '2.6' \n" +
            ")";
    public static final String REDIS_TABLE_SINK1_DDL = "" +
            "CREATE TABLE redis_sink1 (\n" +
            "key1 String,\n" +
            "ress1 String\n" +
            ") WITH (\n" +
            "  'connector.type' = 'redis',  \n" +
            "  'redis.ip' = 'h:p,h:p,h:p',  \n" +
            "  'database.num' = '0', \n"+//不起作用，不知道是不是只有一个数据库的原因
            "  'operate.tpye' = 'string', \n" +
            "  'redis.version' = '2.6' \n" +
            ")";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        DataStream<Tuple2<String,String>> socketDataStream = env.socketTextStream("localhost",9999)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String,String> map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new Tuple2<>(data[0],data[1]);
                    }
                });

        tEnv.sqlUpdate(REDIS_TABLE_DIM_DDL1);
        tEnv.sqlUpdate(REDIS_TABLE_SINK1_DDL);

        //source注册成表
        tEnv.createTemporaryView("data_source", socketDataStream, "id,city,p.proctime");

        Table table = tEnv.sqlQuery("select a.city as c1,b.res as c2 from data_source a \n "+
                "left join redis_dim1 FOR SYSTEM_TIME AS OF a.p AS b on concat('name=',a.city) = b.key");

        tEnv.createTemporaryView("toredis1",table);
        tEnv.executeSql("insert into redis_sink1 select c1,c2 from toredis1");
        env.execute("stringRedis");
    }
}
