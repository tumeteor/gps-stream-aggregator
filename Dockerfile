FROM maven:3.6-jdk-8-alpine AS build
RUN mkdir kafkamisc
ADD . /kafkamisc
WORKDIR /kafkamisc

RUN mvn clean package
RUN apk update && apk add --no-cache libc6-compat

FROM openjdk:8-jdk
COPY --from=build kafkamisc/target/kafka-misc-1.0-SNAPSHOT-jar-with-dependencies.jar kafka-misc-1.0-SNAPSHOT-jar-with-dependencies.jar
EXPOSE 9092:9092
CMD java -cp kafka-misc-1.0-SNAPSHOT-jar-with-dependencies.jar streams.producer.KafkaGPSProducer -k True


#FROM python:3.5
#RUN pip install kafka
#ADD p.py /
#
#CMD [ "python", "./p.py" ]