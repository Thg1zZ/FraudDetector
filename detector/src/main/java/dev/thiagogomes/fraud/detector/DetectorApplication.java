package dev.thiagogomes.fraud.detector;

import java.time.Clock;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;

import dev.thiagogomes.fraud.detector.stream.FraudTopology;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableKafkaStreams
public class DetectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(DetectorApplication.class, args);
    }

    @Bean
    NewTopic transactionsTopic() {
        return TopicBuilder.name(FraudTopology.TRANSACTIONS_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic fraudAlertsTopic() {
        return TopicBuilder.name(FraudTopology.ALERTS_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic transactionsDlt() {
        return TopicBuilder.name(FraudTopology.TRANSACTIONS_DLT).partitions(3).replicas(1).build();
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
