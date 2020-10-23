package org.apache.flink.connector.redis.factory;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.redis.source.RedisLookupTableSource;
import org.apache.flink.connector.redis.util.RedisValidator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.Schema.*;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.connector.redis.util.RedisValidator.*;

/**
 * @ClassName: RedisTableSourceSinkFactory
 * @Description: redis工厂类
 * @author: xiaohai
 * @date: 2020/10/22
 */

public class RedisTableSourceSinkFactory implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Tuple2<Boolean, Row>> {

    //目前不支持redis sink，直接抛出异常
    @Override
    public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
        throw new IllegalArgumentException("目前不支持redis sink");
    }

    //数据源使用，这里维表包含在里面，其实并不是真正意义上的数据源，
    //数据源和维表应该分开的，这里就不分了，但是当做数据源使用后面会报错
    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        //校验参数
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));
        RedisLookupTableSource.Builder builder = RedisLookupTableSource.builder()
                .setFieldNames(schema.getFieldNames())
                .setFieldTypes(schema.getFieldTypes())
                .setConnectIp(descriptorProperties.getString(CONNECT_IP))
                .setDatabaseNum(descriptorProperties.getInt(DATABASE_NUM))
                .setReadType(descriptorProperties.getString(READ_TYPE));

        descriptorProperties.getOptionalLong(LOOKUP_CACHE_MAX_ROWS).ifPresent(builder::setCacheMaxSize);
        descriptorProperties.getOptionalLong(LOOKUP_CACHE_TTL).ifPresent(builder::setCacheExpireMs);

        return builder.build();
    }

    //redis维表 需要参数值是这样的
    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_REDIS);
        //向后兼容
        context.put(CONNECTOR_PROPERTY_VERSION, "1");
        return context;
    }

    //表配置参数
    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        properties.add(CONNECT_IP);
        properties.add(DATABASE_NUM);
        properties.add(READ_TYPE);
        properties.add(LOOKUP_CACHE_MAX_ROWS);
        properties.add(LOOKUP_CACHE_TTL);
        properties.add(REDIS_VERSION);

        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);

        return properties;
    }

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);

        descriptorProperties.putProperties(properties);

        //支持时间时间、支持watermark
        new SchemaValidator(true, false, false).validate(descriptorProperties);

        //校验参数
        new RedisValidator().validate(descriptorProperties);

        return descriptorProperties;
    }
}
