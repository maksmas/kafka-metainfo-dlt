package mmm;

public record RetryMetadata(
        int key,
        String topic,
        int partition,
        long offset,
        long timestamp,
        int retryAttemptNumber) {
}
