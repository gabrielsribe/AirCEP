FROM maven:3.8-jdk-8-slim AS builder

COPY ./pom.xml /opt/pom.xml
COPY ./src /opt/src
RUN cd /opt; mvn clean install -Dmaven.test.skip

FROM apache/flink:1.14.4-scala_2.12-java8

# Download libraries
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.12/1.14.4/flink-sql-connector-kafka_2.12-1.14.4.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc_2.12/1.14.4/flink-connector-jdbc_2.12-1.14.4.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-csv/1.14.4/flink-csv-1.14.4.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-cep_2.11/1.14.4/flink-cep_2.11-1.14.4.jar; 
    #wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-cep_2.11/1.14.4/flink-cep_2.11-1.14.4.jar; \ ##TODOOOOO

COPY --from=builder /opt/target/flink-cep-*.jar /opt/flink/usrlib/flink-cep.jar

RUN echo "execution.checkpointing.interval: 10s" >> /opt/flink/conf/flink-conf.yaml; \
    echo "pipeline.object-reuse: true" >> /opt/flink/conf/flink-conf.yaml; \
    echo "pipeline.time-characteristic: EventTime" >> /opt/flink/conf/flink-conf.yaml; \
    echo "taskmanager.memory.jvm-metaspace.size: 256m" >> /opt/flink/conf/flink-conf.yaml;
