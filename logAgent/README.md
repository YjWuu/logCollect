zookeeper
zookeeper-server-start.bat D:\software\kafka_2.13-3.2.0\config\zookeeper.properties

kafka
kafka-server-start.bat D:\software\kafka_2.13-3.2.0\config\server.properties

读取
kafka-console-consumer.bat --bootstrap-server=127.0.0.1:9092 --topic=web_log --from-beginning