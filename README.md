# AirCEP - Monitoramento da Qualidade do Ar com Processamento de Eventos Complexos

O AirCEP é uma aplicação desenvolvida para monitorar a qualidade do ar em tempo real, utilizando técnicas de Processamento de Eventos Complexos (CEP) e computação na borda. O sistema foi projetado para reduzir o tráfego de rede, otimizar o uso de recursos computacionais e fornecer alertas rápidos sobre condições adversas da qualidade do ar.

## Objetivo

- O objetivo principal do AirCEP é fornecer uma solução eficiente e escalável para o monitoramento da qualidade do ar, com foco em:

- Redução do tráfego de rede: Utilizando filtros de dados configuráveis para enviar apenas informações relevantes.

- Otimização de recursos: Diminuindo o consumo de CPU e memória através do processamento local na borda e filtragem dos dados.

- Detecção de eventos complexos: Identificando padrões críticos de poluição do ar em tempo real.

- Visualização de dados: Oferecendo um painel interativo para acompanhamento das medições e alertas.


## Funcionalidades

- Filtros de dados configuráveis: Permitem definir thresholds para cada poluente e a frequência de envio de dados.

- Processamento de eventos complexos (CEP): Identifica padrões críticos de poluição do ar em tempo real.

- Painel de visualização: Exibe gráficos e alertas em tempo real, facilitando a tomada de decisões.

- Armazenamento de dados históricos: Permite análises retrospectivas e identificação de tendências.


## Tecnologias Utilizadas

- Linguagens de Programação: Java

- Frameworks: Apache Flink (para processamento de streams e CEP)

- Banco de Dados: MySQL (para armazenamento de dados históricos e alertas)

- Message Broker: Apache Kafka (para comunicação entre componentes)

- Visualização de Dados: Grafana (para criação do painel de monitoramento)

- Conteinerização: Docker (para empacotamento e execução dos componentes)

## Como Executar o Projeto
### Pré-requisitos

- Docker e Docker Compose instalados.

- Java JDK 11 ou superior.

- Apache Kafka e Apache Flink configurados (ou rodando via Docker).