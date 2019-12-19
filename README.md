# Plusminus

This is a demonstration application for the [JSON Event Sourcing](https://github.com/json-event-sourcing/pincette-jes) library. It has only one aggregate called ```counter```. The only commands it understands are ```plus``` and ```minus```, which update the field ```value``` in the aggregate.

To try it you need Kafka and MongoDB are running on ```localhost```. You can download the [Confluent platform](https://www.confluent.io/download) and start Kafka with ```confluent start kafka```. Then you have to [install MongoDB](https://docs.mongodb.com/manual/installation/).

You should create the following Kafka topics:

```
audit-dev
plusminus-counter-aggregate-dev
plusminus-counter-command-dev
plusminus-counter-event-dev
plusminus-counter-event-full-dev
plusminus-counter-monitor-dev
plusminus-counter-reply-dev
```

The ```create_topics.sh``` script does this for you.

The configuration of the application is in ```conf/application.conf```.

Build the application with ```mvn clean package``` and start it with ```java -jar target/pincette-plusminus-<version>-jar-with-dependencies.jar```.

To run the tests you have to download and build the repository [pincette-jes-test](https://github.com/json-event-sourcing/pincette-jes-test). Then run it with ```java -jar pincette-jes-test-<version>-jar-with-dependencies.jar -d test```.

