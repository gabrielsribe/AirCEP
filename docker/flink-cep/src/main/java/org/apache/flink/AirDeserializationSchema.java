package org.apache.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;

public class AirDeserializationSchema implements DeserializationSchema<AirRecord> {

    private static final long serialVersionUID = 1L;

    private static final CsvSchema schema = CsvSchema.builder()
            .addColumn("data")
            .addColumn("hora")
            .addColumn("particulasInalaveisMp10")
            .addColumn("particulasInalaveisFinasMp25")
            .addColumn("ozonio")
            .addColumn("monoxidoDeCarbono")
            .addColumn("dioxidoDeNitrogenio")
            .addColumn("timestamp")
            .addColumn("event_transmited_date")
            .addColumn("event_transmited_time")
            .build();

    private static final CsvMapper mapper = new CsvMapper();

    @Override
    public AirRecord deserialize(byte[] message) throws IOException {
        return mapper.readerFor(AirRecord.class).with(schema).readValue(message);
    }

    @Override
    public boolean isEndOfStream(AirRecord nextElement) {
        return false;
    }

    @Override
    public TypeInformation<AirRecord> getProducedType() {
        return TypeInformation.of(AirRecord.class);
    }
}