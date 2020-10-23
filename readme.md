参考https://blog.csdn.net/weixin_47482194/article/details/106528032

建表语句
CREATE TABLE redis_dim (
    key String,
    res Map<String,String>
) WITH (
    'connector.type' = 'redis',
    'redis.ip' = 'ip1:port1,ip2:port2,ip3:port3',
    'database.num' = '0',
    'read.tpye' = 'hash',
    'lookup.cache.max-rows' = '10086',
    'lookup.cache.ttl' = '10086',
    'redis.version' = '2.6'
)

CREATE TABLE redis_dim (
    key String,
    res String
) WITH (
    'connector.type' = 'redis',
    'redis.ip' = 'ip1:port1,ip2:port2,ip3:port3',
    'database.num' = '0',
    'read.tpye' = 'string',
    'lookup.cache.max-rows' = '10086',
    'lookup.cache.ttl' = '10086',
    'redis.version' = '2.6'
)