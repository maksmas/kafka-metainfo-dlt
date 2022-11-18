package mmm;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

class DockerContainer {
    private static final Logger LOG = LoggerFactory.getLogger(DockerContainer.class);
    private static KafkaContainer container;

    static String startKafka(final String mainTopic, final String retriesTopic) {
        container = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.2")).withEmbeddedZookeeper();
        container.start();

        try (
                final var adminClient = AdminClient.create(
                        Map.of(BOOTSTRAP_SERVERS_CONFIG, container.getBootstrapServers()))
        ) {
            LOG.info("Creating main topic - {} and retries topic - {}", mainTopic, retriesTopic);
            adminClient.createTopics(List.of(
                    new NewTopic(mainTopic, 5, (short) 1),
                    new NewTopic(retriesTopic, 1, (short) 1)
            ));

            return container.getBootstrapServers();
        }
    }

    static void stopKafka() {
        if (container != null) {
            container.stop();
        }
    }
}