package org.apache.flink.connector.redis.util;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;

/**
 * @ClassName: RedisValidator
 * @Description: 参数配置、验证类
 * @date: 2020/10/22
 */

public class RedisValidator extends ConnectorDescriptorValidator {

    public static final String CONNECTOR_TYPE_VALUE_REDIS = "redis";

    //集群id和端口
    public static final String CONNECT_IP = "redis.ip";

    //redis数据库
    public static final String DATABASE_NUM = "database.num";

    //读取方式string、hash
    public static final String OPERATE_TYPE = "operate.tpye";

    //缓存条数
    public static final String LOOKUP_CACHE_MAX_ROWS = "lookup.cache.max-rows";

    //缓存过期时间
    public static final String LOOKUP_CACHE_TTL = "lookup.cache.ttl";

    //redis版本，最低为2.6
    public static final String REDIS_VERSION = "redis.version";


    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        //校验连接参数：地址、版本
        validateConnectProperties(properties);
        //校验读取方式：数据库、读取方式
        validateRriteTypeProperties(properties);
        //校验缓存参数：缓存条目数、过期时间
        validateCacheProperties(properties);
    }

    private void validateConnectProperties(DescriptorProperties properties) {
        properties.validateString(CONNECT_IP, false, 1);//地址
        properties.validateDouble(REDIS_VERSION, true, 2.6);//版本
    }

    private void validateRriteTypeProperties(DescriptorProperties properties) {
        properties.validateInt(DATABASE_NUM, false, 0);//数据库
        properties.validateString(OPERATE_TYPE, false, 1);//读取方式
    }

    private void validateCacheProperties(DescriptorProperties properties) {
        properties.validateLong(LOOKUP_CACHE_MAX_ROWS, true);//缓存最大行数
        properties.validateLong(LOOKUP_CACHE_TTL, true);//过期时间

        //校验缓存配置全部为空或部分为空
        checkAllOrNone(properties, new String[]{
                LOOKUP_CACHE_MAX_ROWS,
                LOOKUP_CACHE_TTL
        });
    }

    private void checkAllOrNone(DescriptorProperties properties, String[] propertyNames) {
        int presentCount = 0;
        for (String name : propertyNames) {
            if (properties.getOptionalString(name).isPresent()) {
                presentCount++;
            }
        }
        Preconditions.checkArgument(presentCount == 0 || presentCount == propertyNames.length,
                "应该提供所有或不提供以下属性:\n" + String.join("\n", propertyNames));
    }
}
