package com.learning.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
public class KafkaExampleApplicationMain {

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    public static void main(String[] args) {
        SpringApplication.run(KafkaExampleApplicationMain.class, args);
    }

    @Component
    class MainRunner implements ApplicationRunner {

        public void run(ApplicationArguments args) throws Exception {
            List<String> users = Arrays.asList("david","john","raj","peter");
            List<String> company = Arrays.asList("ibm","apple","google","vmware");
            while (true) {
                kafkaTemplate.send("mytopic", new User(users.get(new Random().nextInt(users.size())),company.get(new Random().nextInt(company.size())),new Random().nextInt(100)));
                TimeUnit.SECONDS.sleep(5);
            }
        }
    }

    @KafkaListener(topics = "mytopic")
    public void receiveTopic1(ConsumerRecord<?, ?> userRecord) {
        log.info("Received : {} ", userRecord);
    }
}

@Data
@AllArgsConstructor
class User{
    String userName,company;
    int salary;
}

