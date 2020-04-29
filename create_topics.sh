kafka-topics --bootstrap-server localhost:9092 --create --topic plusminus-counter-command-dev --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic plusminus-counter-reply-dev --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic plusminus-counter-event-dev --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic plusminus-counter-event-full-dev --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic plusminus-counter-aggregate-dev --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic log-dev --partitions 1 --replication-factor 1
