package org.apache.flink;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.Serializable;
import java.util.Objects;

@JsonPropertyOrder({
        "data",
        "hora",
        "particulasInalaveisMp10",
        "particulasInalaveisFinasMp25",
        "ozonio",
        "monoxidoDeCarbono",
        "dioxidoDeNitrogenio",
        "event_transmited_date",
        "event_transmited_time"
})
@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd hh:mm:ss", timezone = "America/Sao_Paulo")
public class AirRecord implements Serializable {
    public String data;
    public String hora;
    public String particulasInalaveisMp10;
    public String particulasInalaveisFinasMp25;
    public String ozonio;
    public String monoxidoDeCarbono;
    public String dioxidoDeNitrogenio;
    public String event_transmited_date;
    public String event_transmited_time;
    public String timestamp;

    /**
     * A Flink POJO must have a no-args default constructor
     */
    public AirRecord() {
    }

    public AirRecord(String data, String hora, String particulasInalaveisMp10, String particulasInalaveisFinasMp25, String ozonio, String monoxidoDeCarbono, String dioxidoDeNitrogenio
    , String event_transmited_date, String event_transmited_time, String timestamp) {
        this.data = data;
        this.hora = hora;
        this.particulasInalaveisMp10 = particulasInalaveisMp10;
        this.particulasInalaveisFinasMp25 = particulasInalaveisFinasMp25;
        this.ozonio = ozonio;
        this.monoxidoDeCarbono = monoxidoDeCarbono;
        this.dioxidoDeNitrogenio = dioxidoDeNitrogenio;
        this.timestamp = timestamp;
        this.event_transmited_date = event_transmited_date;
        this.event_transmited_time = event_transmited_time;
    }

    @Override
    public String toString() {
        return "Event: {" +
                "data='" + data + '\'' +
                ", hora='" + hora + '\'' +
                ", particulasInalaveisMp10='" + particulasInalaveisMp10 + '\'' +
                ", particulasInalaveisFinasMp25='" + particulasInalaveisFinasMp25 + '\'' +
                ", ozonio='" + ozonio + '\'' +
                ", monoxidoDeCarbono='" + monoxidoDeCarbono + '\'' +
                ", dioxidoDeNitrogenio='" + dioxidoDeNitrogenio + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", event_transmited_date='" + event_transmited_date + '\'' +
                ", event_transmited_time='" + event_transmited_time + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AirRecord record = (AirRecord) o;
        return data == record.data &&
                hora == record.hora &&
                particulasInalaveisMp10 == record.particulasInalaveisMp10 &&
                particulasInalaveisFinasMp25 == record.particulasInalaveisFinasMp25 &&
                ozonio == record.ozonio &&
                monoxidoDeCarbono == record.monoxidoDeCarbono &&
                dioxidoDeNitrogenio == record.dioxidoDeNitrogenio &&
                timestamp == record.timestamp &&
                event_transmited_date == record.event_transmited_date &&
                event_transmited_time == record.event_transmited_time;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                data,
                hora,
                particulasInalaveisMp10,
                particulasInalaveisFinasMp25,
                ozonio,
                monoxidoDeCarbono,
                dioxidoDeNitrogenio,
                timestamp,
                event_transmited_date,
                event_transmited_time
        );
    }

    public double getParticulasInalaveisMp10() {
        return Double.parseDouble(particulasInalaveisMp10);
    }

    public double getParticulasInalaveisFinasMp25() {
        return Double.parseDouble(particulasInalaveisFinasMp25);
    }

    public Double getOzonio() {
        return Double.parseDouble(ozonio);
    }

    public Double getMonoxidoDeCarbono() {
        return Double.parseDouble(monoxidoDeCarbono);
    }

    public Double getDioxidoDeNitrogenio() {
        return Double.parseDouble(dioxidoDeNitrogenio);
    }
}
