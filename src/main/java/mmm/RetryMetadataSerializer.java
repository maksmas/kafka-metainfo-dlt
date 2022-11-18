package mmm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class RetryMetadataSerializer implements Serializer<RetryMetadata> {
    private final ObjectMapper mapper;

    public RetryMetadataSerializer() {
        this.mapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, RetryMetadata data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            System.out.printf("Failed to serialize %s%n", data.toString());
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }
}
