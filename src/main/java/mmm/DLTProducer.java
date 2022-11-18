package mmm;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.sql.Timestamp;
import java.util.Map;

final class DLTProducer {
    private static final Logger LOG = LoggerFactory.getLogger(DLTProducer.class);
    private final KafkaSender<Integer, RetryMetadata> sender;
    private Disposable subscription;
    private final String topic;
    private final Integer ANY_PARTITION = null;

    DLTProducer(final String bootstrapServers, final String topic) {
        this.topic = topic;
        sender = KafkaSender.create(SenderOptions.create(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.CLIENT_ID_CONFIG, "retries-producer",
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RetryMetadataSerializer.class)
        ));
    }

    void send(RetryMetadata retryMetadata) {
        final var currentTimestamp = new Timestamp(System.currentTimeMillis()).getTime();

        final Flux<SenderRecord<Integer, RetryMetadata, Integer>> outboundFlux = Flux.just(
                SenderRecord.create(
                        topic, ANY_PARTITION, currentTimestamp, retryMetadata.key(), retryMetadata, retryMetadata.key()
                )
        );

        subscription = sender.send(outboundFlux)
                .doOnError(err -> {
                    LOG.error("Failed to publish retry message {}", retryMetadata, err);
                })
                .subscribe(r -> {
                    RecordMetadata metadata = r.recordMetadata();

                    LOG.info(
                            "Message with key {} sent to retry topic successfully, topic-partition={}-{} offset={}",
                            retryMetadata.key(),
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset()
                    );
                });
    }

    void stop() {
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }

        if (sender != null) {
            sender.close();
        }
    }
}
