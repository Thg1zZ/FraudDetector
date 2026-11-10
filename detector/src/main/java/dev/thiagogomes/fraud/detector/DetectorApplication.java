package dev.thiagogomes.fraud.detector;

import java.time.Clock;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import dev.thiagogomes.fraud.detector.stream.FraudTopology;
import dev.thiagogomes.fraud.detector.alert.FraudAlert;

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
    NewTopic alertsDlt() {
        return TopicBuilder.name(FraudTopology.ALERTS_DLT).partitions(3).replicas(1).build();
    }

    @Bean
    KafkaTemplate<String, Object> recoveryTemplate(KafkaProperties properties) {
        var serializer = new DelegatingByTypeSerializer(Map.of(
                byte[].class, new ByteArraySerializer(),
                FraudAlert.class, new JacksonJsonSerializer<FraudAlert>().noTypeInfo()));
        var config = properties.buildProducerProperties();
        config.put("acks", "all");
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config, new StringSerializer(), serializer));
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> recoveryTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(recoveryTemplate,
                (record, exception) -> new TopicPartition(FraudTopology.ALERTS_DLT, record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
