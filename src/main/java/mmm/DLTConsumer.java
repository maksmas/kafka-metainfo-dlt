package mmm;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;

final class DLTConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(DLTConsumer.class);
    private final KafkaReceiver<Integer, RetryMetadata> dltKafkaReceiver;
    private final KafkaConsumer<Integer, String> retriesConsumer;
    private Disposable subscription;

    DLTConsumer(final String bootstrapServers, final String topic) {
        final ReceiverOptions<Integer, RetryMetadata> receiverOptions = ReceiverOptions.create(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.CLIENT_ID_CONFIG, "dlt-consumer",
                        ConsumerConfig.GROUP_ID_CONFIG, "dlt-group",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, RetryMetadataDeSerializer.class,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                )
        );

        final var options = receiverOptions.subscription(List.of(topic))
                .addAssignListener(partitions -> LOG.info("onPartitionsAssigned {}", partitions))
                .addRevokeListener(partitions -> LOG.info("onPartitionsRevoked {}", partitions));

        dltKafkaReceiver = KafkaReceiver.create(options);

        retriesConsumer = new KafkaConsumer(
                retryConsumerProperties(bootstrapServers),
                new IntegerDeserializer(),
                new StringDeserializer()
        );
    }

    void retryMessagesInDLT() {
        final var kafkaFlux = dltKafkaReceiver.receive();
        this.subscription = kafkaFlux.subscribe(record -> {
            final var offset = record.receiverOffset();
            LOG.info("Received retry instruction: key={} value={}", record.key(), record.value());
            retryMessage(record.value());
            offset.acknowledge();
        });
    }

    void stopConsuming() {
        if (this.subscription != null && !this.subscription.isDisposed()) {
            this.subscription.dispose();
        }

        if (retriesConsumer != null) {
            retriesConsumer.close();
        }
    }

    private void retryMessage(final RetryMetadata retryInfo) {
        final var tp = new TopicPartition(retryInfo.topic(), retryInfo.partition());
        retriesConsumer.assign(List.of(tp));
        retriesConsumer.seek(tp, retryInfo.offset());
        final var messageFromMainTopicToRetry = retriesConsumer.poll(Duration.ofMillis(1_000)).iterator();

        if (!messageFromMainTopicToRetry.hasNext()) {
            LOG.error("No message found in main topic for retry instruction {}", retryInfo);
        } else {
            final var messageToRetry = messageFromMainTopicToRetry.next();

            LOG.info("Retry consumer got {} - {}", messageToRetry.key(), messageToRetry.value());
            // todo call processor
        }
    }

    private Map<String, String> retryConsumerProperties(final String bootstrapServers) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.CLIENT_ID_CONFIG, "retries-consumer",
                ConsumerConfig.GROUP_ID_CONFIG, "retries-group"
        );
    }
}
