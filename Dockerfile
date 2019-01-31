FROM maven:3.5-jdk-8-alpine AS build
RUN mkdir kafkamisc
ADD . /kafkamisc
WORKDIR /kafkamisc

RUN mvn clean package

#FROM openjdk:8
#COPY --from=build kafkamisc/target/kafka-misc-1.0-SNAPSHOT-jar-with-dependencies.jar kafka-misc-1.0-SNAPSHOT-jar-with-dependencies.jar
EXPOSE 9092:9092
CMD java -cp target/kafka-misc-1.0-SNAPSHOT-jar-with-dependencies.jar StringSer.KafkaGPSProducer


#FROM python:3.5
#RUN pip install kafka
#ADD p.py /
#
#CMD [ "python", "./p.py" ]