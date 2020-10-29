package org.apache.flink.connector.redis.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Map;

public class RedisAppendTableSink implements AppendStreamTableSink<Row> {
    private final TableSchema schema;
    private final Map<String, String> properties;

    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;

    private final String connectIp;
    private final String operateType;

    private final long cacheMaxSize;
    private final long cacheExpireMs;

    private RedisAppendTableSink(TableSchema schema, Map<String, String> properties,
                                 String[] fieldNames, TypeInformation[] fieldTypes,
                                 String connectIp, String operateType,
                                 long cacheMaxSize, long cacheExpireMs){
        this.schema = schema;
        this.properties = properties;

        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;

        this.connectIp = connectIp;
        this.operateType = operateType;

        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        System.out.println("开始跳转到sink function");

        return dataStream.addSink(new RedisHashAppendTableFunction(connectIp, operateType))
                .setParallelism(dataStream.getParallelism());
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return this.schema.toRowType();
    }

    @Override
    public String[] getFieldNames() {
        return this.fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return this.fieldTypes;
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)){
            throw new ValidationException("Reconfiguration with different fields is not allowed. " +
                    "Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
                    "But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
        }
        return this;
    }

    public static class Builder {
        private TableSchema schema;
        private Map<String, String> properties;

        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        private String connectIp;
        private String operateType;

        private long cacheMaxSize;
        private long cacheExpireMs;

        public Builder setTableSchema(TableSchema schema){
            this.schema = schema;
            return this;
        }
        public Builder setProperties(Map<String, String> properties){
            this.properties = properties;
            return this;
        }
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
        public Builder setOperateType(String operateType) {
            this.operateType = operateType;
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
        public RedisAppendTableSink build(){
            return new RedisAppendTableSink(schema, properties, fieldNames, fieldTypes,
                    connectIp, operateType,cacheMaxSize, cacheExpireMs);
        }
    }

}
