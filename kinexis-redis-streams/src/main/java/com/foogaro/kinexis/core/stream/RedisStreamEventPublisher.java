package com.foogaro.kinexis.core.stream;

import com.foogaro.kinexis.core.config.KinexisProperties;
import com.foogaro.kinexis.core.model.KinexisEvent;
import com.foogaro.kinexis.core.telemetry.KinexisTelemetry;
import com.foogaro.kinexis.core.telemetry.SimpleKinexisTelemetry;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Map;
import java.util.Objects;

public class RedisStreamEventPublisher implements EventPublisher {

    private final RedisTemplate<String, String> redisTemplate;
    private final StreamPartitioner streamPartitioner;
    private final KinexisTelemetry telemetry;

    public RedisStreamEventPublisher(RedisTemplate<String, String> redisTemplate) {
        this(redisTemplate, new StreamPartitioner(new KinexisProperties()), new SimpleKinexisTelemetry());
    }

    public RedisStreamEventPublisher(RedisTemplate<String, String> redisTemplate, StreamPartitioner streamPartitioner) {
        this(redisTemplate, streamPartitioner, new SimpleKinexisTelemetry());
    }

    public RedisStreamEventPublisher(RedisTemplate<String, String> redisTemplate,
                                     StreamPartitioner streamPartitioner,
                                     KinexisTelemetry telemetry) {
        this.redisTemplate = redisTemplate;
        this.streamPartitioner = streamPartitioner;
        this.telemetry = telemetry;
    }

    @Override
    public String append(Class<?> entityType, KinexisEvent event) {
        String streamKey = streamPartitioner.streamKey(entityType, event);
        RecordId recordId = redisTemplate.opsForStream().add(StreamRecords.newRecord()
                .withId(RecordId.autoGenerate())
                .ofMap(event.toRecordMap())
                .withStreamKey(streamKey));
        if (recordId != null) {
            telemetry.increment(KinexisTelemetry.STREAM_EVENTS_PUBLISHED, Map.of(
                    "entity", entityType.getSimpleName(),
                    "operation", event.operation().getValue(),
                    "stream", streamKey));
        }
        return Objects.nonNull(recordId) ? recordId.getValue() : null;
    }
}
