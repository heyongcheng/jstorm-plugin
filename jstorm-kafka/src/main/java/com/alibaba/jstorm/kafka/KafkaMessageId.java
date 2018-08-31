package com.alibaba.jstorm.kafka;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author heyc
 * @date 2018/8/24 14:05
 */
@Setter
@Getter
public class KafkaMessageId implements Comparable<KafkaMessageId>, Serializable{

    private static final long serialVersionUID = 5356185540554465605L;

    private String topic;

    private int partition;

    private long offset;

    public KafkaMessageId() {
    }

    public KafkaMessageId(String topic, int partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public int compareTo(final KafkaMessageId kafkaMessageId) {
       return kafkaMessageId == null ? 1 : Long.valueOf(this.offset - kafkaMessageId.offset).intValue();
    }

    @Override
    public String toString() {
        return "KafkaMessageId{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                '}';
    }
}
