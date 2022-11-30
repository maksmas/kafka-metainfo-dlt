# Kafka DLT without copying message content to DLT

Retry failed message processing without writing them to DLT.

Instead of writing message contents to DLT, message metadata is written to DLT.

Metadata includes message topic, partition and offset.


Message is retried by consuming message position from DLT and consuming message from main topic. 

### Steps
1. Start kafka container `DockerContainer::startKafka`
2. Create consumer for the main topic `MainConsumer`. It depends on `DLTProducer` for publishing unprocessed message to DLT
3. Start `DLTConsumer` for processing failed messages and retrying them
4. Start `MainProducer` for publishing message to main topic
5. `MainProducer` will publish messages to main topic
6. `MainConsumer` will consume messages and process them. Some message processing will fail. [Information about failed messages](./src/main/java/mmm/RetryMetadata.java) will be published to DLT
7. `DLTConsumer` will consume information about failed messages. Based on information dltConsumer will seek specific message at main topic and retry processing
8. Tear down

