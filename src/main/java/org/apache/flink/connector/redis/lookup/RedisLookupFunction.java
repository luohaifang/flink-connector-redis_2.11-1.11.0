package org.apache.flink.connector.redis.lookup;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: RedisLookupFunction
 * @Description: 取数的工人
 * @date: 2020/10/22
 */

public class RedisLookupFunction extends AsyncTableFunction<Row> {

    private static final Logger log = LoggerFactory.getLogger(RedisLookupFunction.class);

    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;

    private final String connectIp;
    private final int databaseNum;
    private final String readType;

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private transient Cache<String, String> cache;
    private transient Cache<String, Map<String, String>> cacheHash;

    //cluster
    RedisClusterClient clusterClient;
    StatefulRedisClusterConnection<String, String> clusterConnection;
    RedisAdvancedClusterAsyncCommands<String, String> clusterAsyncCommands;

    //构造函数
    public RedisLookupFunction(String[] fieldNames, TypeInformation[] fieldTypes,
                               String connectIp, int databaseNum, String readType,
                               long cacheMaxSize, long cacheExpireMs) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;

        this.connectIp = connectIp;
        this.databaseNum = databaseNum;
        this.readType = readType;

        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        private String connectIp;
        private int databaseNum;
        private String readType;

        private long cacheMaxSize;
        private long cacheExpireMs;

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder setConnectIp(String connectIp) {
            this.connectIp = connectIp;
            return this;
        }

        public Builder setDatabaseNum(int databaseNum) {
            this.databaseNum = databaseNum;
            return this;
        }

        public Builder setReadType(String readType) {
            this.readType = readType;
            return this;
        }

        public Builder setCacheMaxSize(long cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        public Builder setCacheExpireMs(long cacheExpireMs) {
            this.cacheExpireMs = cacheExpireMs;
            return this;
        }

        public RedisLookupFunction build() {
            return new RedisLookupFunction(fieldNames, fieldTypes,
                    connectIp, databaseNum, readType,
                    cacheMaxSize, cacheExpireMs);
        }
    }

    //返回类型
    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    //扫尾工作，关闭连接
    @Override
    public void close() throws Exception {
        if (cache != null || cacheHash != null){
            cache.cleanUp();
            cacheHash.cleanUp();
        }
        if (clusterClient != null)
            clusterClient.shutdown();

        super.close();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        try {
            //初始化redis异步客户端
            ArrayList<RedisURI> list = new ArrayList<>();
            String[] ips = connectIp.split(",");
            for (String ip : ips) {
                list.add(RedisURI.create("redis://"+ip+"/"+databaseNum));
            }
            clusterClient = RedisClusterClient.create(list);
            clusterConnection = clusterClient.connect();
            clusterAsyncCommands= clusterConnection.async();

        } catch (Exception e) {
            throw new Exception("建立redis异步客户端失败", e);
        }

        try {
            //初始化缓存大小
            if ("hash".equals(readType)) {
                this.cacheHash = cacheMaxSize <= 0 || cacheExpireMs <= 0 ? null : CacheBuilder.newBuilder()
                        .expireAfterWrite(cacheExpireMs, TimeUnit.SECONDS)
                        .maximumSize(cacheMaxSize)
                        .build();
            }
            else if ("string".equals(readType)) {
                this.cache = cacheMaxSize <= 0 || cacheExpireMs <= 0 ? null : CacheBuilder.newBuilder()
                        .expireAfterWrite(cacheExpireMs, TimeUnit.SECONDS)
                        .maximumSize(cacheMaxSize)
                        .build();
            }
            else {
                throw new IllegalArgumentException("没有这种类型读取方式");
            }
            log.info("cache is null ? :{}", cache == null);
        } catch (Exception e) {
            throw new Exception("创建cache失败", e);

        }
    }

    //取数据，先从缓存拿，是否保存空结果，以防止这个key反复来查redis、导致雪崩
    public void eval(CompletableFuture<Collection<Row>> future, String key) {
        if ("hash".equals(readType)){
            //先从缓存查
            if (cacheHash != null) {
                Map<String,String> value = cacheHash.getIfPresent(key);
                log.info("缓存中的值为null:{}", value == null);
                if (value != null) {
                    future.complete(Collections.singletonList(Row.of(key,value)));
                    return;
                }
            }
            try {
                clusterAsyncCommands.hgetall(key).thenAccept(value -> {
                    //是否为空
                    if (value == null) {
                        value.put("","");
                    }
                    //是否缓存
                    if (cacheHash != null)
                        cacheHash.put(key, value);
                    future.complete(Collections.singletonList(Row.of(key,value)));
                });
            }catch (Exception e){
                log.error("从redis拿数据失败", e);
                throw new RuntimeException("从redis拿数据失败", e);
            }

        }
        else if ("string".equals(readType)){
            //先从缓存查
            if (cache != null) {
                String value = cache.getIfPresent(key);
                log.info("缓存中的值为null:{}", value == null);
                if (value != null) {
                    future.complete(Collections.singletonList(Row.of(key, value)));
                    return;
                }
            }
            try {
                clusterAsyncCommands.get(key).thenAccept(value -> {
                    //是否为空
                    if (value == null) {
                        value = "";
                    }
                    //是否缓存
                    if (cache != null)
                        cache.put(key, value);
                    future.complete(Collections.singletonList(Row.of(key, value)));
                });
            }catch (Exception e){
                log.error("从redis拿数据失败", e);
                throw new RuntimeException("从redis拿数据失败", e);
            }
        }

    }

}
