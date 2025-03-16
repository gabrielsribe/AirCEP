package org.apache.flink.sensor.datagen.model;

import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import java.time.format.DateTimeFormatter;

public class QualidadeArSerializer implements Serializer<QualidadeAr> {

    private static final DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
  
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}
  
    @Override
    public byte[] serialize(String topic, QualidadeAr dadosQualidadeAr) {
        String csv =
            String.format(
            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
            dadosQualidadeAr.data,
            dadosQualidadeAr.hora,
            dadosQualidadeAr.particulasInalaveisMp10,
            dadosQualidadeAr.particulasInalaveisFinasMp25,
            dadosQualidadeAr.ozonio,
            dadosQualidadeAr.monoxidoDeCarbono,
            dadosQualidadeAr.dioxidoDeNitrogenio,
            dadosQualidadeAr.timestamp,
            dadosQualidadeAr.event_transmited_date,
            dadosQualidadeAr.event_transmited_time

        );
  
      return csv.getBytes();
    }
  
    @Override
    public void close() {}
  }