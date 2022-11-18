package mmm;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

final class MainConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(MainConsumer.class);
    private final KafkaReceiver<Integer, String> kafkaReceiver;
    private final DLTProducer dltProducer;
    private Disposable subscription;

    MainConsumer(final String bootstrapServers, final String topic, final DLTProducer dltProducer) {
        this.dltProducer = dltProducer;
        final ReceiverOptions<Integer, String> receiverOptions = ReceiverOptions.create(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.CLIENT_ID_CONFIG, "main-consumer",
                        ConsumerConfig.GROUP_ID_CONFIG, "main-group",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                )
        );

        final var options = receiverOptions.subscription(List.of(topic))
                .addAssignListener(partitions -> LOG.info("onPartitionsAssigned {}", partitions))
                .addRevokeListener(partitions -> LOG.info("onPartitionsRevoked {}", partitions));

        kafkaReceiver = KafkaReceiver.create(options);
    }

    void consumeMessages() {
        final var kafkaFlux = kafkaReceiver.receive();
        this.subscription = kafkaFlux.subscribeOn(Schedulers.newParallel("mainConsumerScheduler", 5, true))
                .subscribe(record -> {
                    final var offset = record.receiverOffset();
                    LOG.info(
                            "Received message: topic-partition={} offset={} key={} value={}",
                            offset.topicPartition(),
                            offset.offset(),
                            record.key(),
                            record.value()
                    );

                    // todo send random records to DLT with random delay
                    if (record.key() % 4 == 0) {
                        dltProducer.send(
                                new RetryMetadata(
                                        record.key(),
                                        record.topic(),
                                        record.partition(),
                                        record.offset(),
                                        record.timestamp(),
                                        1
                                )
                        );
                    }

                    offset.acknowledge();
                });
    }

    void stopConsumption() {
        if (this.subscription != null && !this.subscription.isDisposed()) {
            this.subscription.dispose();
        }

        dltProducer.stop();
    }
}
