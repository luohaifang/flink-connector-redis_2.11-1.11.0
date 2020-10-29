package org.apache.flink.connector.redis;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class test12 {
    public static final String REDIS_TABLE_DIM_DDL = "" +
            "CREATE TABLE redis_dim (\n" +
            "key String,\n" +
            "res Map<String,String>\n" +
            ") WITH (\n" +
            "  'connector.type' = 'redis',  \n" +
            "  'redis.ip' = '192.168.129.114:16379,192.168.129.115:16379,192.168.129.116:16379',  \n" +
            "  'database.num' = '0', \n"+//不起作用，不知道是不是只有一个数据库的原因
            "  'operate.tpye' = 'hash', \n" +
            "  'lookup.cache.max-rows' = '3', \n" +
            "  'lookup.cache.ttl' = '50', \n" +
            "  'redis.version' = '2.6' \n" +
            ")";

    public static final String REDIS_TABLE_SINK_DDL = "" +
            "CREATE TABLE redis_sink (\n" +
            "key String,\n" +
            "field String,\n" +
            "ress String\n" +
            ") WITH (\n" +
            "  'connector.type' = 'redis',  \n" +
            "  'redis.ip' = '192.168.129.114:17379,192.168.129.115:17379,192.168.129.116:17379',  \n" +
            "  'database.num' = '0', \n"+//不起作用，不知道是不是只有一个数据库的原因
            "  'operate.tpye' = 'hash', \n" +
            "  'redis.version' = '2.6' \n" +
            ")";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        DataStream<Tuple2<String,String>> socketDataStream = env.socketTextStream("localhost",6666)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String,String> map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new Tuple2<>(data[0],data[1]);
                    }
                });

        //注册redis维表
        tEnv.sqlUpdate(REDIS_TABLE_DIM_DDL);
        tEnv.sqlUpdate(REDIS_TABLE_SINK_DDL);

        //source注册成表
        tEnv.createTemporaryView("data_source", socketDataStream, "id,city,p.proctime");

        //join语句
        Table table = tEnv.sqlQuery("select a.id as c1,b.res['en'] as c2,b.res['ch'] as c3 from data_source a \n "+
                "left join redis_dim FOR SYSTEM_TIME AS OF a.p AS b on concat('name=',a.city) = b.key");

        //执行写redis
        tEnv.createTemporaryView("toredis",table);
        tEnv.executeSql("insert into redis_sink select c1,c2,c3 from toredis");

        env.execute("hashRedis");

    }
}
