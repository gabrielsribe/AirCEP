package org.apache.flink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.TopicPartition;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class AlertJob {

    private static final int PARTICULAS_INALAVEIS_MP10_THRESHOLD = 50;

    private static final int PARTICULAS_INALAVEIS_FINAS_MP25_THRESHOLD = 25;

    private static final int OZONIO_O3_THRESHOLD = 100;

    private static final int MONOXIDO_DE_CARBONO_THRESHOLD = 9;

    private static final int DIOXIDO_DE_NITROGENIO_NO2_THRESHOLD = 200;

    public AlertJob() throws Exception {

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf); //REMOTE
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf); //LOCAL TEST
        
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        env.setParallelism(1);

        KafkaSource<AirRecord> source = KafkaSource.<AirRecord>builder()
                .setBootstrapServers("kafka:9092") //inside docker kafka:9092
                //.setBootstrapServers("localhost:9094") //outside docker localhost:9094
                .setTopics("air")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AirDeserializationSchema())
                .build();

        DataStream<AirRecord> stream = env.fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)), "Kafka Source");

        //Sync RAW data
        stream.addSink(
                JdbcSink.sink(
                        "INSERT INTO qualidade_ar " +
                                "(data, " +
                                "hora, " +
                                "particulasInalaveisMp10," +
                                "particulasInalaveisFinasMp25," +
                                "ozonio, " +
                                "monoxidoDeCarbono," +
                                "dioxidoDeNitrogenio," +
                                "event_transmited_date," +
                                "event_transmited_time," +
                                "timestamp)" +
                                "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (statement, data) -> {
                            statement.setString(1, data.data);
                            statement.setString(2, data.hora);
                            statement.setString(3, data.particulasInalaveisMp10);
                            statement.setString(4, data.particulasInalaveisFinasMp25);
                            statement.setString(5, data.ozonio);
                            statement.setString(6, data.monoxidoDeCarbono);
                            statement.setString(7, data.dioxidoDeNitrogenio);
                            statement.setString(8, data.event_transmited_date);
                            statement.setString(9, data.event_transmited_time);
                            statement.setString(10, data.timestamp);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(5000)
                                .withBatchIntervalMs(1)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://mysql:3306/AirCEP") //job inside docker
                                //.withUrl("jdbc:mysql://localhost:3306/AirCEP")// job outside docker
                                .withUsername("aircep")
                                .withPassword("aircep")
                                .build()
                )).name("RAW DATA").uid("raw-data-sink");;

        //DEBUG
        //stream.print();

        //CEP
        //Padroes de qualidade
        /*
        * N1(0-40) Boa , N2(41-80) Moderada, N3(81-120), N4(121-200) Muito Ruim, N5(>200) Pessima
        * Mp10: 0-50 (n1), >50 – 100 (n2), >100 – 150 (n3), >150 – 250 (n4), >250 (n5)
        * Mp25: 0-25 (n1), >25 – 50 (n2), >50 – 75 (n3), >75 – 125 (n4), >125 (n5)
        * O3: 0-100 (n1), >100 – 130 (n2), >130 – 160 (n3), >160 – 200 (n4), >200 (n5)
        * CO: 0-9 (n1), >9 – 11 (n2),  >11 – 13 (n3), >13 – 15 (n4), >15 (n5)
        * NO2: 0-200 (n1), >200 – 240 (n2), >240 – 320 (n3), >320 – 1130 (n4), >1130 (n5)
        * SO2: 0-20 (n1), >20 – 40 (n2), >40 – 365 (n3), >365 – 800 (n4), >800 (n5)
        * */
        Pattern<AirRecord, ?> pattern = Pattern.<AirRecord>begin("start")
                .where(new SimpleCondition<AirRecord>() {
                    @Override
                    public boolean filter(AirRecord value) throws Exception {
                        return value.getParticulasInalaveisMp10() > PARTICULAS_INALAVEIS_MP10_THRESHOLD ||
                                value.getParticulasInalaveisFinasMp25() > PARTICULAS_INALAVEIS_FINAS_MP25_THRESHOLD ||
                                value.getOzonio() > OZONIO_O3_THRESHOLD ||
                                value.getMonoxidoDeCarbono() > MONOXIDO_DE_CARBONO_THRESHOLD ||
                                value.getDioxidoDeNitrogenio() > DIOXIDO_DE_NITROGENIO_NO2_THRESHOLD;
                    }
                }).times(1);//.within(Time.seconds(10)) WINDOW FUNCTION SETUP

        PatternStream<AirRecord> patternStream = CEP.pattern(stream, pattern);

        //Alert Pattern
        DataStream<AlertAir> alerts = patternStream.select(new PatternSelectFunction<AirRecord, AlertAir>() {
            @Override
            public AlertAir select(Map<String, List<AirRecord>> pattern) {
                LocalDateTime now = LocalDateTime.now();
                DateTimeFormatter dateFormatterTime = DateTimeFormatter.ofPattern("HH:mm:ss.SSSS");
                String alert_detection_time = now.format(dateFormatterTime);

                AirRecord startEvent = pattern.get("start").get(0);
                //AirRecord endEvent = pattern.get("start").get(0);
                //if you have multiple eventts in tthe paternn they're defined in the patternStream
                //AirRecord endEvent = pattern.get("end").get(0);
                return new AlertAir(startEvent, alert_detection_time);
            }
        });

        //DEBUG
        //alerts.print();
        alerts.addSink(
                JdbcSink.sink(
                        "INSERT INTO alertas_ar " +
                                "(data, " +
                                "hora, " +
                                "particulasInalaveisMp10," +
                                "particulasInalaveisFinasMp25," +
                                "ozonio, " +
                                "monoxidoDeCarbono," +
                                "dioxidoDeNitrogenio," +
                                "event_transmited_date," +
                                "event_transmited_time," +
                                "timestamp," +
                                "alert_detection_time)" +
                                "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (statement, alert) -> {
                            statement.setString(1, alert.record_1.data);
                            statement.setString(2, alert.record_1.hora);
                            statement.setString(3, alert.record_1.particulasInalaveisMp10);
                            statement.setString(4, alert.record_1.particulasInalaveisFinasMp25);
                            statement.setString(5, alert.record_1.ozonio);
                            statement.setString(6, alert.record_1.monoxidoDeCarbono);
                            statement.setString(7, alert.record_1.dioxidoDeNitrogenio);
                            statement.setString(8, alert.record_1.event_transmited_date);
                            statement.setString(9, alert.record_1.event_transmited_time);
                            statement.setString(10, alert.record_1.timestamp);
                            statement.setString(11, alert.alert_detection_time);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(5000)
                                .withBatchIntervalMs(1)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://mysql:3306/AirCEP")
                                //.withUrl("jdbc:mysql://localhost:3306/AirCEP")// job outside docker
                                //.withDriverName("com.mysql.jdbc.Drive")
                                .withUsername("aircep")
                                .withPassword("aircep")
                                .build()
                )).name("ALERT DATA").uid("alert-data-sink");;

        //DEBUG
        alerts.print();
        System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
        env.execute(AlertJob.class.getSimpleName());
    }

    private static class MySimpleStringSchema extends SimpleStringSchema {
        private static final long serialVersionUID = 1L;
        private final String SHUTDOWN = "SHUTDOWN";

        @Override
        public String deserialize(byte[] message) {

            return super.deserialize(message);
        }

        @Override
        public boolean isEndOfStream(String nextElement) {
            if (SHUTDOWN.equalsIgnoreCase(nextElement)) {
                return true;
            }
            return super.isEndOfStream(nextElement);
        }
    }

    public static void main(String[] args) throws Exception {
        new AlertJob();
    }

}
