package mmm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private final static String MAIN_TOPIC = "main_topic";
    private final static String RETRIES_TOPIC = "retries_topic";

    public static void main(String[] args) throws InterruptedException {
        LOG.info("Starting kafka container");
        final var kafkaBootstrapServer = DockerContainer.startKafka(MAIN_TOPIC, RETRIES_TOPIC);

        final var dltProducer = new DLTProducer(kafkaBootstrapServer, RETRIES_TOPIC);
        LOG.info("Starting main consumer");
        final var mainConsumer = new MainConsumer(kafkaBootstrapServer, MAIN_TOPIC, dltProducer);
        mainConsumer.consumeMessages();
        LOG.info("Started main consumer");

        LOG.info("Starting DLR consumer");
        final var dltConsumer = new DLTConsumer(kafkaBootstrapServer, RETRIES_TOPIC);
        dltConsumer.retryMessagesInDLT();
        LOG.info("Started DLR consumer");

        LOG.info("Starting main publisher");
        final var producer = new MainProducer(kafkaBootstrapServer, MAIN_TOPIC);
        producer.sendNumbers(100);
        LOG.info("Started publisher");

        Thread.sleep(15_000);
        LOG.info("Stopping producer");
        producer.stop();
        LOG.info("Stopping consumers");
        mainConsumer.stopConsumption();
        dltConsumer.stopConsuming();

        LOG.info("Stopping kafka container");
        DockerContainer.stopKafka();
    }
}