package com.foogaro.kinexis.core.stream;

import com.foogaro.kinexis.core.Misc;
import com.foogaro.kinexis.core.model.KinexisEvent;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Objects;

public class RedisStreamEventPublisher implements EventPublisher {

    private final RedisTemplate<String, String> redisTemplate;

    public RedisStreamEventPublisher(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public String append(Class<?> entityType, KinexisEvent event) {
        RecordId recordId = redisTemplate.opsForStream().add(StreamRecords.newRecord()
                .withId(RecordId.autoGenerate())
                .ofMap(event.toRecordMap())
                .withStreamKey(Misc.getStreamKey(entityType)));
        return Objects.nonNull(recordId) ? recordId.getValue() : null;
    }
}
