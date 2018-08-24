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
import java.util.concurrent.ConcurrentHashMap;

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

    private volatile Map<String, SortedSet<KafkaMessageId>> pendingOffsetsMap;

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
        this.pendingOffsetsMap = new ConcurrentHashMap<String, SortedSet<KafkaMessageId>>(1000);
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
                collector.emit(new Values(record.value()), messageId);
            }
        }
        // commit offset
        long now = System.currentTimeMillis();
        if((now - lastUpdateMs) > this.offsetUpdateIntervalMs) {
            this.commitOffset();
        }
    }

    /**
     * commitOffset
     */
    protected void commitOffset() {
        Iterator<Map.Entry<String, SortedSet<KafkaMessageId>>> iterator = pendingOffsetsMap.entrySet().iterator();
        while (iterator.hasNext()) {
            SortedSet<KafkaMessageId> messageIds = iterator.next().getValue();
            for (KafkaMessageId messageId : messageIds) {
                Map<TopicPartition, OffsetAndMetadata> consumed = new HashMap<TopicPartition, OffsetAndMetadata>();
                consumed.put(new TopicPartition(messageId.getTopic(), messageId.getPartition()), new OffsetAndMetadata(messageId.getOffset()));
                this.kafkaConsumer.commitSync(consumed);
            }
        }
    }

    @Override
    public void ack(final Object msgId) {
        if (!enableAutoCommit) {
            KafkaMessageId messageId = (KafkaMessageId)msgId;
            String offsetKey = messageId.getTopic() + "-" + messageId.getPartition();
            SortedSet<KafkaMessageId> sortedSet = pendingOffsetsMap.get(offsetKey);
            if (sortedSet == null) {
                synchronized (this) {
                    sortedSet = pendingOffsetsMap.get(offsetKey);
                    if (sortedSet == null) {
                        sortedSet = new TreeSet<KafkaMessageId>();
                        pendingOffsetsMap.put(offsetKey, sortedSet);
                    }
                }
            }
            sortedSet.add(messageId);
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
