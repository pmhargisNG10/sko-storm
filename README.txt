Kafka
--create events
/usr/lib/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tagevent

--list all events
/usr/lib/kafka/bin/kafka-topics.sh --list -zookeeper localhost:2181

HBase
--
create 'tag_meta_data', 'm', 's'

--
create 'tag_events', 't', 'q', 'c'

-- holds tag_id (string), pump_id(string), alert type=(alarm|error string), message(string)
create 'tag_alerts', 'a'


Hive
---
CREATE EXTERNAL TABLE tag_alerts(
  key string,
  alertType string,
  message string,desc
  tagId string,
  pumpId bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,a:alertType,a:message,a:tagId,a:pumpId")
TBLPROPERTIES (
  "hbase.table.name" = "t_tag_alerts",
 "hbase.table.default.storage.type" = "binary"
  );

#meta data sample data:
<tag_id>|<pump_id>|high|low|hhigh|llow|uom|desc
