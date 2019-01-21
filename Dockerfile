FROM java:8
WORKDIR /
add target/kafka-misc-1.0-SNAPSHOT-jar-with-dependencies.jar kafka-misc-1.0-SNAPSHOT-jar-with-dependencies.jar
EXPOSE 9092:9092
CMD java -cp kafka-misc-1.0-SNAPSHOT-jar-with-dependencies.jar StringSer.KafkaGPSProducer


