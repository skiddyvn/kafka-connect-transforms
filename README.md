Build: docker run --rm -v "$(pwd)":/app -w /app maven:3.9-eclipse-temurin-17 mvn clean package

Download: curl -sSL -O https://github.com/skiddyvn/kafka-connect-transforms/releases/download/1.0.0/kafka-connect-transforms-0.1.0-SNAPSHOT.jar

Input
SinkRecord{kafkaOffset=948409, timestampType=CreateTime}
ConnectRecord{
    topic='cdc_users', 
    kafkaPartition=3, 
    key=Struct{id=499353}, 
    keySchema=Schema{STRUCT}, 
    value=Struct{_id=11,name=Van,full_name=,status=1,addresses=[Struct{name=1,description=}, Struct{name=2,description=}]
}, 
valueSchema=Schema{users:STRUCT}, timestamp=1738825370213, headers=ConnectHeaders(headers=)}   [com.github.skiddyvn.kafka.connect.transforms.DataTypeFilter]