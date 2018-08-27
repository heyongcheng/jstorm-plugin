package com.alibaba.jstorm.kafka;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author heyc
 * @date 2018/8/24 10:30
 */
public class KafkaSpout<K, V> implements IRichSpout {

    private static final long serialVersionUID = 7472243757546572812L;

    private static final Logger logger = LoggerFactory.getLogger(KafkaSpout.class);

    private SpoutOutputCollector collector;

    private KafkaConfig kafkaConfig;

    private boolean enableAutoCommit;

    private long pollTimeout;

    private long lastUpdateMs;

    private long offsetUpdateIntervalMs;

    private volatile PartitionPendingCoordinator partitionPendingCoordinator;

    private KafkaConsumer<K, V> kafkaConsumer;

    public KafkaSpout(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public KafkaSpout(String configPath) {
        this(new KafkaConfig(configPath));
    }

    @Override
    public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        this.collector = collector;
        this.kafkaConfig.config(conf);
        logger.info("kafkaConsumer config: {}", kafkaConfig.toJSONString());
        this.lastUpdateMs = System.currentTimeMillis();
        this.enableAutoCommit = this.kafkaConfig.getBoolean(KafkaConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        this.pollTimeout = this.kafkaConfig.getLong(KafkaConfig.POLL_TIMEOUT, 100L);
        this.offsetUpdateIntervalMs = this.kafkaConfig.getLong(KafkaConfig.OFFSET_UPDATE_INTERVALMS, 100L);
        this.kafkaConsumer = new KafkaConsumer<K, V>(this.kafkaConfig.getProperties());
        this.partitionPendingCoordinator = new PartitionPendingCoordinator();
        this.kafkaConsumer.subscribe(this.kafkaConfig.getTopics());
    }

    @Override
    public void close() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        ConsumerRecords<K, V> records = kafkaConsumer.poll(pollTimeout);
        if (records != null && !records.isEmpty()) {
            for (ConsumerRecord<K, V> record : records) {
                KafkaMessageId messageId = new KafkaMessageId(record.topic(), record.partition(), record.offset());
                // 添加到待提交队列
                if (!enableAutoCommit) {
                    PartitionPendingOffset pendingOffset = partitionPendingCoordinator.getPendingOffset(record.topic(), record.partition());
                    pendingOffset.addPendingOffsets(record.offset());
                    pendingOffset.setEmittingOffset(record.offset());
                }
                collector.emit(new Values(record.value()), messageId);
            }
        }
        // commit offset
        if(!enableAutoCommit && (System.currentTimeMillis() - lastUpdateMs) > this.offsetUpdateIntervalMs) {
            this.commitOffset();
        }
    }

    /**
     * commitOffset
     */
    protected void commitOffset() {
        lastUpdateMs = System.currentTimeMillis();
        Collection<PartitionPendingOffset> partitionPendingOffsets = partitionPendingCoordinator.getPendingOffsetMap().values();
        if (!partitionPendingOffsets.isEmpty()) {
            List<PartitionPendingOffset> commiteds = new ArrayList<PartitionPendingOffset>();
            Map<TopicPartition, OffsetAndMetadata> consumed = new HashMap<TopicPartition, OffsetAndMetadata>();
            for (PartitionPendingOffset partitionPendingOffset : partitionPendingOffsets) {
                if (partitionPendingOffset.getCommitingOffset() != partitionPendingOffset.getLastCommittedOffset()) {
                    commiteds.add(partitionPendingOffset);
                    consumed.put(new TopicPartition(partitionPendingOffset.getTopic(), partitionPendingOffset.getPartition()), new OffsetAndMetadata(partitionPendingOffset.getCommitingOffset()));
                }
            }
            if (!commiteds.isEmpty()) {
                this.kafkaConsumer.commitSync(consumed);
                for (PartitionPendingOffset commitedOffset : commiteds) {
                    commitedOffset.setLastCommittedOffset(commitedOffset.getCommitingOffset());
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("commit offset: {}", commiteds);
                }
            }
        }
    }

    @Override
    public void ack(final Object msgId) {
        if (!enableAutoCommit) {
            KafkaMessageId messageId = (KafkaMessageId)msgId;
            partitionPendingCoordinator.getPendingOffset(messageId.getTopic(), messageId.getPartition()).remove(messageId.getOffset());
        }
    }

    @Override
    public void fail(final Object msgId) {
        logger.error("message fail: {}", JSONObject.toJSONString(msgId));
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("bytes"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
