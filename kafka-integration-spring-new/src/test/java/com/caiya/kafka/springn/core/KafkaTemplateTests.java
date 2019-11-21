package com.caiya.kafka.springn.core;

import com.caiya.kafka.springn.Application;
import com.caiya.kafka.springn.support.SendResult;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * KafkaTemplateTests
 *
 * @author wangnan
 * @since 1.0.0, 2019/11/8
 **/
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class KafkaTemplateTests {

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    @Resource
    private KafkaTemplate<String, String> manualCommitKafkaTemplate;

    @Test
    public void sendDefaultTest() throws ExecutionException, InterruptedException {
        SendResult<String, String> result = kafkaTemplate.sendDefault("Hello, world.").get();
        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getRecordMetadata());
    }

    @Test
    public void test() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(20);
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            executor.execute(() -> {
                kafkaTemplate.sendDefault("Hello, world. " + finalI);
                manualCommitKafkaTemplate.sendDefault("Hello, world. " + finalI);
            });
        }

        TimeUnit.SECONDS.sleep(3);
    }

}
