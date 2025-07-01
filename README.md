1In debezium-connector, Add the following configuration:

```properties
connector.class=io.debezium.connector.mysql.MySqlConnector
# ...
converters: 'datetime,decimal'
datetime.type: com.av.debezium.converter.MySqlDateTimeConverter
datetime.format.time: 'HH:mm:ss'
datetime.format.date: 'yyyy-MM-dd'
datetime.format.timestamp: 'yyyy-MM-dd HH:mm:ss'
datetime.format.timestamp.zone: UTC+8
decimal.type: com.av.debezium.converter.MySqlDecimalConverter
decimal.databaseType: mysql
datetime.format.datetime: 'yyyy-MM-dd HH:mm:ss'
```
