package org.apache.flink.connector.redis.sink;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
public class RedisHashAppendTableFunction extends RichSinkFunction<Row>  {//implements CheckpointedFunction
    private static final Logger LOG = LoggerFactory.getLogger(RedisHashAppendTableFunction.class);

    private final String connectIp;
    private final String operateType;

    //cluster
    private transient RedisClusterClient clusterClient;
    private transient StatefulRedisClusterConnection<String, String> clusterConnection;
    private transient RedisAdvancedClusterAsyncCommands<String, String> clusterAsyncCommands;

    public RedisHashAppendTableFunction(String connectIp, String operateType) {
        this.connectIp = connectIp;
        this.operateType = operateType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            //初始化redis异步客户端
            System.out.println("sink初始化redis异步客户端");
            ArrayList<RedisURI> list = new ArrayList<>();
            String[] ips = connectIp.split(",");
            for (String ip : ips) {
                list.add(RedisURI.create("redis://"+ip));
            }
            clusterClient = RedisClusterClient.create(list);
            clusterConnection = clusterClient.connect();
            clusterAsyncCommands= clusterConnection.async();
        } catch (Exception e) {
            throw new Exception("建立redis异步客户端失败", e);
        }
//        super.open(parameters);
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        try {
            if ("hash".equals(operateType)){
                clusterAsyncCommands.hset(value.getField(0).toString(),
                        value.getField(1).toString(),value.getField(2).toString());
            }else if ("string".equals(operateType)){
                clusterAsyncCommands.set(value.getField(0).toString(),value.getField(1).toString());
            }
        }catch (Exception e){
            LOG.error("写redis失败", e);
            throw new RuntimeException("写redis失败", e);
        }

        System.out.println("sink结果"+value.toString());
    }

    @Override
    public void close() throws Exception {
        if (clusterConnection != null)
            clusterConnection.close();
        super.close();
    }


}
