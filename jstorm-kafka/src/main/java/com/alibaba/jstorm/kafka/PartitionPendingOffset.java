package com.alibaba.jstorm.kafka;

import lombok.Getter;
import lombok.Setter;

import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author heyc
 * @date 2018/8/27 9:43
 */
@Setter
@Getter
public class PartitionPendingOffset {

    private String topic;

    private int partition;

    private long emittingOffset;

    private long lastCommittedOffset;

    private SortedSet<Long> pendingOffsets = new ConcurrentSkipListSet<Long>();

    public PartitionPendingOffset(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    /**
     * getCommitingOffset
     * @return
     */
    public Long getCommitingOffset() {
        if (pendingOffsets.isEmpty() || pendingOffsets.size() <= 0) {
            return emittingOffset;
        }
        try {
            return pendingOffsets.first();
        } catch (Exception e) {
            return emittingOffset;
        }
    }

    /**
     * isEmpty
     * @return
     */
    public boolean isEmpty() {
        return pendingOffsets.isEmpty();
    }

    /**
     * size
     * @return
     */
    public long size() {
        return pendingOffsets.size();
    }

    /**
     * remove
     * @param offset
     */
    public void remove(Long offset) {
        pendingOffsets.remove(offset);
    }

    /**
     * addPendingOffsets
     * @param offset
     */
    public void addPendingOffsets(Long offset) {
        pendingOffsets.add(offset);
    }

    @Override
    public String toString() {
        return "PartitionPendingOffset{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", emittingOffset=" + emittingOffset +
                ", lastCommittedOffset=" + lastCommittedOffset +
                '}';
    }
}