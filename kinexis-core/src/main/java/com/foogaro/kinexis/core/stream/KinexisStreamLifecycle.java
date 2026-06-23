package com.foogaro.kinexis.core.stream;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

public class KinexisStreamLifecycle {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final RedisTemplate<String, String> redisTemplate;
    private final StreamMessageListenerContainer<String, MapRecord<String, String, String>> listenerContainer;
    private final Set<String> initializedGroups = ConcurrentHashMap.newKeySet();

    public KinexisStreamLifecycle(
            RedisTemplate<String, String> redisTemplate,
            StreamMessageListenerContainer<String, MapRecord<String, String, String>> listenerContainer) {
        this.redisTemplate = redisTemplate;
        this.listenerContainer = listenerContainer;
    }

    public void ensureConsumerGroup(String streamKey, String groupName) {
        String key = streamKey + ":" + groupName;
        if (!initializedGroups.add(key)) {
            return;
        }
        try {
            ensureStreamExists(streamKey);
            redisTemplate.opsForStream().createGroup(streamKey, ReadOffset.from("0"), groupName);
            logger.info("Consumer group {} created for stream {}", groupName, streamKey);
        } catch (RuntimeException e) {
            String message = e.getMessage() == null ? "" : e.getMessage();
            if (message.contains("BUSYGROUP")) {
                logger.debug("Consumer group {} already exists for stream {}", groupName, streamKey);
                return;
            }
            initializedGroups.remove(key);
            throw e;
        }
    }

    public void receive(String streamKey, String groupName, String consumerName,
                        StreamListener<String, MapRecord<String, String, String>> listener) {
        ensureConsumerGroup(streamKey, groupName);
        listenerContainer.receive(
                Consumer.from(groupName, consumerName),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
                listener
        );
        if (!listenerContainer.isRunning()) {
            listenerContainer.start();
        }
    }

    private void ensureStreamExists(String streamKey) {
        if (Boolean.TRUE.equals(redisTemplate.hasKey(streamKey))) {
            return;
        }
        try {
            redisTemplate.opsForStream().add(StreamRecords.newRecord()
                    .in(streamKey)
                    .ofMap(Map.of("init", "true")));
        } catch (RuntimeException e) {
            if (!Boolean.TRUE.equals(redisTemplate.hasKey(streamKey))) {
                throw e;
            }
        }
    }
}
