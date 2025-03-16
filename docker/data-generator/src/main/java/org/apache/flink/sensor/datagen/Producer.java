package org.apache.flink.sensor.datagen;

import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.apache.flink.sensor.datagen.model.QualidadeAr;
import org.apache.flink.sensor.datagen.model.QualidadeArSupplier;
import org.apache.flink.sensor.datagen.model.QualidadeArSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

/** Generates CSV transaction records at a rate */
public class Producer implements Runnable, AutoCloseable {

  private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");

  private volatile boolean isRunning;

  private final String brokers;

  private final String topic;

  private static long lastSendTime = System.currentTimeMillis();

  private static final long TIME_INTERVAL_RECORD_SEND = 10000;  // 5 seconds (MILISECONDS)

  private static final int PARTICULAS_INALAVEIS_MP10_THRESHOLD = 50;

  private static final int PARTICULAS_INALAVEIS_FINAS_MP25_THRESHOLD = 25;

  private static final int OZONIO_O3_THRESHOLD = 100;

  private static final int MONOXIDO_DE_CARBONO_THRESHOLD = 9;

  private static final int DIOXIDO_DE_NITROGENIO_NO2_THRESHOLD = 200;

  private static final int SO2_THRESHOLD = 20;

  private static final boolean FILTERED_DATA = false;

  public Producer(String brokers, String topic) {
    this.brokers = brokers;
    this.topic = topic;
    this.isRunning = true;
  }

  @Override
  public void run() {
    KafkaProducer<String, QualidadeAr> producer = new KafkaProducer<>(getProperties());

    Throttler throttler = new Throttler(5000); /*Defaul = 100, unlimited = -1 */

    QualidadeArSupplier dadosMeteorologicos = new QualidadeArSupplier();

    while (isRunning) {

        QualidadeAr dadoMeteorologico = dadosMeteorologicos.get();

        ProducerRecord<String, QualidadeAr> record = new ProducerRecord<>(topic, null, null, null, dadoMeteorologico);

        if (FILTERED_DATA) {

          // just send anomalous data or if a certain amount of data has passed
          if (Double.parseDouble(dadoMeteorologico.particulasInalaveisMp10) > PARTICULAS_INALAVEIS_MP10_THRESHOLD ||
            Double.parseDouble(dadoMeteorologico.particulasInalaveisFinasMp25) > PARTICULAS_INALAVEIS_FINAS_MP25_THRESHOLD ||
            Double.parseDouble(dadoMeteorologico.ozonio) > OZONIO_O3_THRESHOLD ||
            Double.parseDouble(dadoMeteorologico.monoxidoDeCarbono) > MONOXIDO_DE_CARBONO_THRESHOLD ||
            Double.parseDouble(dadoMeteorologico.dioxidoDeNitrogenio) > DIOXIDO_DE_NITROGENIO_NO2_THRESHOLD ||
            hasTimeElapsed()) 
            {
              producer.send(record);
              lastSendTime = System.currentTimeMillis();
              try {
                throttler.throttle();
              } catch (InterruptedException e) {
                isRunning = false;
              }
            }
          else if (hasTimeElapsed()) {
            producer.send(record); 
            lastSendTime = System.currentTimeMillis();
              try {
                throttler.throttle();
              } catch (InterruptedException e) {
                isRunning = false;
              }
          }

        }
        else {
          producer.send(record); 
          try {
            throttler.throttle();
          } catch (InterruptedException e) {
            isRunning = false;
          }
        }
        
      }

    producer.close();
  }

  @Override
  public void close() {
    isRunning = false;
  }

  private Properties getProperties() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, QualidadeArSerializer.class);
    return props;
  }

  private static boolean hasTimeElapsed() {
    long currentTime = System.currentTimeMillis();
    return (currentTime - lastSendTime) >= TIME_INTERVAL_RECORD_SEND;
}
}
