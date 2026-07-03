package com.foogaro.kinexis.core.stream;

import com.foogaro.kinexis.core.Misc;
import com.foogaro.kinexis.core.config.KinexisProperties;
import com.foogaro.kinexis.core.model.KinexisEvent;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32;
import java.util.stream.IntStream;

public class StreamPartitioner {

    public static final String PARTITION_SUFFIX = ":partition:";

    private final KinexisProperties properties;

    public StreamPartitioner(KinexisProperties properties) {
        this.properties = properties;
    }

    public String streamKey(Class<?> entityType, KinexisEvent event) {
        int partitionCount = partitionCount();
        if (partitionCount == 1) {
            return Misc.getStreamKey(entityType);
        }
        String partitionKey = event.entityId().orElse(event.eventId());
        return streamKey(entityType, partition(partitionKey, partitionCount));
    }

    public List<String> streamKeys(Class<?> entityType) {
        int partitionCount = partitionCount();
        if (partitionCount == 1) {
            return List.of(Misc.getStreamKey(entityType));
        }
        return IntStream.range(0, partitionCount)
                .mapToObj(partition -> streamKey(entityType, partition))
                .toList();
    }

    public int partitionCount() {
        return Math.max(1, properties.getStream().getPartitions());
    }

    private String streamKey(Class<?> entityType, int partition) {
        return Misc.getStreamKey(entityType) + PARTITION_SUFFIX + partition;
    }

    private int partition(String partitionKey, int partitionCount) {
        CRC32 crc32 = new CRC32();
        crc32.update(partitionKey.getBytes(StandardCharsets.UTF_8));
        return (int) Math.floorMod(crc32.getValue(), partitionCount);
    }
}
