package org.apache.flink.sensor.datagen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

  private static final String KAFKA = "localhost:9094"; //outside = localhost:9094 //innside container = "kafka:9092"

  private static final String TOPIC = "air"; //old = "transactions";

  public static void main(String[] args) {
    Producer producer = new Producer(KAFKA, TOPIC);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Shutting down");
                  producer.close();
                }));

    producer.run();
  }
}
