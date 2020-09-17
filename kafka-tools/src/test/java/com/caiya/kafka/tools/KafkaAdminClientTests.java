package com.caiya.kafka.tools;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * Kafka admin client Test.
 *
 * @author wangnan
 * @since 1.0.0, 2020/5/21
 **/
public class KafkaAdminClientTests {

    private AdminClient adminClient = null;

    @Before
    public void before() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9093,127.0.0.1:9094");
        adminClient = AdminClient.create(props);
    }


    @Test
    public void testCreateTopics() throws ExecutionException, InterruptedException {
        String topicName = "test-1";
        NewTopic newTopic = new NewTopic(topicName, 2, (short) 2);
        Collection<NewTopic> newTopicList = new ArrayList<>();
        newTopicList.add(newTopic);
        adminClient.createTopics(newTopicList);
        assertTrue(adminClient.listTopics().names().get().contains(topicName));
        adminClient.deleteTopics(Collections.singleton(topicName));
    }

    @After
    public void after() {
        adminClient.close();
    }


}
