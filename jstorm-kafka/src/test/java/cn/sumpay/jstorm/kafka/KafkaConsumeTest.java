package cn.sumpay.jstorm.kafka;

import com.alibaba.jstorm.kafka.KafkaConfig;
import com.alibaba.jstorm.kafka.KafkaMessageId;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author heyc
 * @date 2018/8/24 14:30
 */
public class KafkaConsumeTest {

    private static ConcurrentLinkedQueue comsumeSet = new ConcurrentLinkedQueue();

    public static void main(String[] args) {
        KafkaConfig kafkaConfig = new KafkaConfig("classpath:application.yaml");
        KafkaConsumer<Object, Object> kafkaConsumer = new KafkaConsumer<>(kafkaConfig.getProperties());
        kafkaConsumer.subscribe(kafkaConfig.getTopics());
        while (true) {
            ConsumerRecords<Object, Object> records = kafkaConsumer.poll(100);
            if (records != null && !records.isEmpty()) {
                for (ConsumerRecord record : records) {
                    System.out.println("record: " + record.value());
                    KafkaMessageId messageId = new KafkaMessageId(record.topic(), record.partition(), record.offset());
                    comsumeSet.add(messageId);
                    Map<TopicPartition, OffsetAndMetadata> consumed = new HashMap<TopicPartition, OffsetAndMetadata>();
                    consumed.put(new TopicPartition(messageId.getTopic(), messageId.getPartition()), new OffsetAndMetadata(messageId.getOffset()));
                    kafkaConsumer.commitSync(consumed);
                }
            }
        }
    }


}
