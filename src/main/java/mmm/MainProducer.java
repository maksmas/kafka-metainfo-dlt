package mmm;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.LocalDateTime;
import java.util.Map;

final class MainProducer {
    private static final Logger LOG = LoggerFactory.getLogger(MainProducer.class);
    private final KafkaSender<Integer, String> sender;
    private Disposable senderSubscription;
    private final String topic;

    MainProducer(final String bootstrapServers, final String topic) {
        this.topic = topic;
        sender = KafkaSender.create(SenderOptions.create(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.CLIENT_ID_CONFIG, "main-producer",
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
        ));
    }

    void sendNumbers(final int count) {
        try {
            senderSubscription = sender.send(Flux.range(0, count).map(i -> SenderRecord.create(
                            new ProducerRecord<>(topic, i % 5, i, LocalDateTime.now().toString()),
                            i
                    )))
                    .doOnError(System.err::println)
                    .subscribe(r -> {
                        final var metadata = r.recordMetadata();

                        LOG.info(
                                "Message {} sent successfully, topic-partition={}-{} offset={}",
                                r.correlationMetadata(),
                                metadata.topic(),
                                metadata.partition(),
                                metadata.offset()
                        );
                    });
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    void stop() {
        if (senderSubscription != null && !senderSubscription.isDisposed()) {
            senderSubscription.dispose();
        }

        if (sender != null) {
            sender.close();
        }
    }
}
