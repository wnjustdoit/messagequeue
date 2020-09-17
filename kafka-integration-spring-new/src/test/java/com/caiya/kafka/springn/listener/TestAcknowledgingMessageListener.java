package com.caiya.kafka.springn.listener;

import com.caiya.kafka.springn.component.KafkaProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collection;

/**
 * TestAcknowledgingMessageListener.
 * </p>
 * 手动提交测试。
 * </p>
 * 可以暂存{@link ConsumerRecord}到内存中，直至数量超过指定的值或者超过指定的时间必须提交（批量）；
 * 可以通过分区遍历进行消费（如果分区订阅，必须分区手动提交）。
 *
 * @author wangnan
 * @see <a href="http://kafka.apache.org/23/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html">KafkaConsumer</a>
 * @since 1.0.0, 2019/11/19
 **/
@Component
public class TestAcknowledgingMessageListener implements AcknowledgingMessageListener<String, String> {

    @Resource
    private KafkaProperties manualCommitKafkaProperties;

    /**
     * 当业务需要严格的消费顺序，且当前消息必须消费成功时，才能消费接下来的消息。
     * 可以做个标记是否需要下一次的消费，或者直接抛出异常，那么消费线程将终结
     */
    private volatile boolean flag = true;

    @Override
    public void onMessage(ConsumerRecords<String, String> data, Consumer<?, ?> consumer) {

        if (!flag)
            return;
        // 消费方式一：
//        logger().info("======records=====:" + JSON.toJSONString(data));
        for (ConsumerRecord<String, String> record : data) {
            try {
                logger().info("======record=====:" + record);
                if (record.value().contains("aaa")) {
                    continue;
                }
            } catch (Exception e) {
                e.printStackTrace();
//                send msg 告警通知
                flag = false;// 或者直接抛出异常，中断消费线程（下次重启触发新的消费时，直接从已提交的offset+1继续消费）
            }
            consumer.commitSync();
        }

        // 消费方式二：
//        for (TopicPartition partition : data.partitions()) {
//            List<ConsumerRecord<String, String>> partitionRecords = data.records(partition);
//            for (ConsumerRecord<String, String> record : partitionRecords) {
//                System.out.println(record.offset() + ": " + record.value());
//            }
//            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
//            consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
//        }
    }

    @Override
    public long pollTimeoutInMillis() {
        return 2000;
    }

    @Override
    public Collection<String> topics() {
        return manualCommitKafkaProperties.getTopics();
    }

    @Override
    public String consumerFactoryName() {
        return "manualCommitConsumerFactory";
    }

    @Override
    public Collection<Integer> partitions() {
        return null;
    }

}
