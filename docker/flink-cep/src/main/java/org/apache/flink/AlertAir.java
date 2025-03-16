package org.apache.flink;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class AlertAir implements Serializable {

    //Create Alert class:
    public AlertAir(AirRecord record_1, String alert_detection_time) {
        this.record_1 = record_1;
        //this.record_2 = record_2;
        this.alert_detection_time = alert_detection_time;
    }

    public AirRecord record_1;
    public AirRecord record_2;

    public String alert_detection_time;

    public String alert;

    @Override
    public String toString() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter dateFormatterDate = DateTimeFormatter.ofPattern("dd/MM/YYYY");
        String event_detected_date = now.format(dateFormatterDate);

        DateTimeFormatter dateFormatterTime = DateTimeFormatter.ofPattern("HH:mm:ss.SSSS");
        String event_detected_time = now.format(dateFormatterTime);

        return "Event 1: {" +
                "data='" + record_1.data + '\'' +
                ", hora='" + record_1.hora + '\'' +
                ", particulasInalaveisMp10='" + record_1.particulasInalaveisMp10 + '\'' +
                ", particulasInalaveisFinasMp25='" + record_1.particulasInalaveisFinasMp25 + '\'' +
                ", ozonio='" + record_1.ozonio + '\'' +
                ", dioxidoDeNitrogenio='" + record_1.dioxidoDeNitrogenio + '\'' +
                ", timestamp='" + record_1.timestamp + '\'' +
                ", event_transmited_date='" + record_1.event_transmited_date + '\'' +
                ", event_transmited_time='" + record_1.event_transmited_time + '\'' +
                ", event_detected_date='" + event_detected_date + '\'' +
                ", event_detected_time='" + event_detected_time + '\'' +
                '}' /*+ "\n" +
                "Event 2: {" +
                "data='" + record_2.data + '\'' +
                ", hora='" + record_2.hora + '\'' +
                ", particulasInalaveisMp10='" + record_2.particulasInalaveisMp10 + '\'' +
                ", particulasInalaveisFinasMp25='" + record_2.particulasInalaveisFinasMp25 + '\'' +
                ", ozonio='" + record_2.ozonio + '\'' +
                ", dioxidoDeNitrogenio='" + record_2.dioxidoDeNitrogenio + '\'' +
                ", event_detected_date='" + event_detected_date + '\'' +
                ", event_detected_time='" + event_detected_time + '\'' +
                '}'*/;
    }
}