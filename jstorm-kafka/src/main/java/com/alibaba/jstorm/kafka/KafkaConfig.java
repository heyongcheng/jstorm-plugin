package com.alibaba.jstorm.kafka;

import com.alibaba.jstorm.common.utils.ResourceUtils;
import com.alibaba.jstorm.common.utils.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.Serializable;
import java.util.*;

/**
 * @author heyc
 * @date 2018/8/24 10:46
 */
public class KafkaConfig implements Serializable{

    private static final long serialVersionUID = -5050753505318514016L;

    public static final String DEFAULT_TOPIC = "jstorm";

    public static final String TOPIC_KEY = "kafka.topic";

    public static final String TOPICS_KEY = "kafka.topics";

    public static final String ENABLE_AUTO_COMMIT_CONFIG = ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

    public static final String POLL_TIMEOUT = "kafka.poll.timeout";

    public static final String OFFSET_UPDATE_INTERVALMS = "kafka.offset.update.intervalms";

    private static final Set<String> CONFIG_KEYS = new HashSet<String>();

    protected Properties properties;

    static {
        CONFIG_KEYS.add(ConsumerConfig.GROUP_ID_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.FETCH_MAX_BYTES_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.METADATA_MAX_AGE_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.SEND_BUFFER_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.RECEIVE_BUFFER_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.CLIENT_ID_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.CHECK_CRCS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG);
        CONFIG_KEYS.add(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG);
        CONFIG_KEYS.add(POLL_TIMEOUT);
        CONFIG_KEYS.add(OFFSET_UPDATE_INTERVALMS);
    }

    public KafkaConfig() {
    }

    public KafkaConfig(Properties properties) {
        this.properties = properties;
        if (!this.properties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            this.properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        }
        if (!this.properties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            this.properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        }
    }

    public KafkaConfig(String configPath) {
        this(ResourceUtils.readAsProperties(configPath));
    }

    /**
     * getTopics
     * @return
     */
    public List<String> getTopics() {
        String topic = this.properties.getProperty(TOPIC_KEY);
        if (StringUtils.isNotEmpty(topic)) {
            return Arrays.asList(topic);
        }
        String topics = this.properties.getProperty(TOPICS_KEY);
        if (StringUtils.isNotEmpty(topics)) {
            return Arrays.asList(topics.split(";|,"));
        }
        return Arrays.asList(DEFAULT_TOPIC);
    }

    /**
     * config
     * @param conf
     */
    public void config(Map<String, ?> conf) {
        if (conf != null && !conf.isEmpty()) {
            Iterator<? extends Map.Entry<String, ?>> iterator = conf.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, ?> next = iterator.next();
                this.config(next.getKey(), next.getValue());
            }
        }
    }

    /**
     * config
     * @param key
     * @param value
     */
    public void config(String key, Object value) {
        if (CONFIG_KEYS.contains(key)) {
            this.properties.put(key, value);
        }
    }

    /**
     * getValue
     * @param key
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> T getValue(String key, Class<T> clazz) {
        return (T)this.properties.get(key);
    }

    /**
     * getValue
     * @param key
     * @param defaultValue
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> T getValue(String key, T defaultValue, Class<T> clazz) {
        return (T)this.properties.getOrDefault(key, defaultValue);
    }

    /**
     * getString
     * @param key
     * @return
     */
    public String getString(String key) {
        return this.properties.getProperty(key);
    }

    /**
     * getString
     * @param key
     * @param defaultValue
     * @return
     */
    public String getString(String key, String defaultValue) {
        return this.properties.getProperty(key, defaultValue);
    }

    /**
     * getLong
     * @param key
     * @return
     */
    public Long getLong(String key) {
        Object value = this.properties.get(key);
        return value == null ? null : Long.valueOf(value.toString());
    }

    /**
     * getLong
     * @param key
     * @param defaultValue
     * @return
     */
    public Long getLong(String key, Long defaultValue) {
        Long value = getLong(key);
        return value == null ? defaultValue : value;
    }

    /**
     * getBoolean
     * @param key
     * @return
     */
    public Boolean getBoolean(String key) {
        Object value = this.properties.get(key);
        return value == null ? null : Boolean.valueOf(value.toString());
    }

    /**
     * getBoolean
     * @param key
     * @param defaultValue
     * @return
     */
    public Boolean getBoolean(String key, Boolean defaultValue) {
        Boolean value = this.getBoolean(key);
        return value == null ? defaultValue : value;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return this.properties.toString();
    }
}
