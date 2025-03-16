package org.apache.flink.sensor.datagen.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class QualidadeArSupplier implements Supplier<QualidadeAr> {

    private final Iterator<QualidadeAr> iterator;
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final String filePathLocal = "/home/gabriel/Projects/flink-env/docker/data-generator/src/main/java/org/apache/flink/sensor/datagen/model/dados_cetesb_ribeirao_mili.csv";
    private static final String filePathDocker = "/dados_cetesb_ribeirao_mili.csv";
    
    public QualidadeArSupplier() {
        try {
            File file = new File(filePathLocal);
            if(!file.exists()){
                file = new File(filePathDocker);
            }
            BufferedReader reader = new BufferedReader(new FileReader(file));
            Stream<String> lines = reader.lines().skip(1); // Skip header line
            iterator = lines.map(this::parseLine).iterator();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private QualidadeAr parseLine(String line) {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter dateFormatterDate = DateTimeFormatter.ofPattern("dd/MM/YYYY");
        String event_transmited_date = now.format(dateFormatterDate);

        DateTimeFormatter dateFormatterTime = DateTimeFormatter.ofPattern("HH:mm:ss.SSSS");
        String event_transmited_time = now.format(dateFormatterTime);

        String[] fields = line.split(",");
        QualidadeAr dados = new QualidadeAr();
        dados.data = fields[0];
        dados.hora = fields[1];
        dados.particulasInalaveisMp10 = fields[2];
        dados.particulasInalaveisFinasMp25 = fields[3];
        dados.ozonio = fields[4];
        dados.monoxidoDeCarbono = fields[5];
        dados.dioxidoDeNitrogenio = fields[6];
        dados.timestamp = fields[7];
        dados.event_transmited_date = event_transmited_date;
        dados.event_transmited_time = event_transmited_time;
        return dados;
    }

    @Override
    public QualidadeAr get() {

        if (!iterator.hasNext()) {
            // Exit the program when the records are over
            System.exit(0); // Exit with a status code (0 for success)
        }
        
        QualidadeAr dadoQualidadeAr = new QualidadeAr();
        dadoQualidadeAr = iterator.next();
        return dadoQualidadeAr;
    }
}
