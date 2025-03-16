CREATE TABLE alertas_ar (
    data VARCHAR(255),
    hora VARCHAR(255),
    particulasInalaveisMp10 VARCHAR(255),
    particulasInalaveisFinasMp25 VARCHAR(255),
    ozonio VARCHAR(255),
    monoxidoDeCarbono VARCHAR(255),
    dioxidoDeNitrogenio VARCHAR(255),
    event_transmited_date VARCHAR(255),
    event_transmited_time VARCHAR(255),
    timestamp VARCHAR(255),
	alert_detection_time VARCHAR(255),
    PRIMARY KEY (data, hora, timestamp)
);

CREATE TABLE qualidade_ar (
    data VARCHAR(255),
    hora VARCHAR(255),
    particulasInalaveisMp10 VARCHAR(255),
    particulasInalaveisFinasMp25 VARCHAR(255),
    ozonio VARCHAR(255),
    monoxidoDeCarbono VARCHAR(255),
    dioxidoDeNitrogenio VARCHAR(255),
    event_transmited_date VARCHAR(255),
    event_transmited_time VARCHAR(255),
    timestamp VARCHAR(255),
    PRIMARY KEY (data, hora, timestamp)
);
