参考文献  
参考https://blog.csdn.net/weixin_47482194/article/details/106528032

flink版本：1.11.0  

redis模式：cluster  
redis api：lettuce  

阔以上传至私服或本地仓库  
```pom.xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-redis_2.11</artifactId>
    <version>1.0</version>
</dependency>
```

与redis维表join，支持hash、string  
```join sql
select a.numberx,b.res as enname, a.p from kafkatable as a left join redis_dim FOR SYSTEM_TIME AS OF a.p as b on concat('source:no=',a.numberx)=b.key and 'en'=b.hashkey
```
sink to redis，支持hash、string  
```insert sql
tEnv.executeSql("insert into redis_sink select a1,'chname',chname from restable");
```

亲测59000条数据\~看起来6得起飞\~
