package com.alibaba.jstorm.kafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author heyc
 * @date 2018/8/27 9:36
 */
public class PartitionPendingCoordinator {

    private Map<String, PartitionPendingOffset> pendingOffsetMap = new ConcurrentHashMap<String, PartitionPendingOffset>();

    /**
     * getPendingOffset
     * @param topic
     * @param partition
     * @return
     */
    public PartitionPendingOffset getPendingOffset(String topic, int partition) {
        String offsetKey = topic + "-" + partition;
        PartitionPendingOffset pendingOffset = pendingOffsetMap.get(offsetKey);
        if (pendingOffset == null) {
            synchronized (this) {
                pendingOffset = pendingOffsetMap.get(offsetKey);
                if (pendingOffset == null) {
                    pendingOffset = new PartitionPendingOffset(topic, partition);
                    pendingOffsetMap.put(offsetKey, pendingOffset);
                }
            }
        }
        return pendingOffset;
    }

    /**
     * getPendingOffsetMap
     * @return
     */
    public Map<String, PartitionPendingOffset> getPendingOffsetMap() {
        return pendingOffsetMap;
    }
}
