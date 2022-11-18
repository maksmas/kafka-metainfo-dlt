package mmm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class RetryMetadataDeSerializer implements Deserializer<RetryMetadata> {
    private final ObjectMapper mapper;

    public RetryMetadataDeSerializer() {
        this.mapper = new ObjectMapper();
    }

    @Override
    public RetryMetadata deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, RetryMetadata.class);
        } catch (IOException e) {
            System.out.printf("Failed to deserialize message%n");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
