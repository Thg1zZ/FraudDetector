package dev.thiagogomes.fraud.detector.stream;

import java.time.Clock;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import dev.thiagogomes.fraud.detector.alert.FraudAlert;
import dev.thiagogomes.fraud.detector.config.FraudRulesProperties;
import dev.thiagogomes.fraud.detector.rule.VelocityRule;
import dev.thiagogomes.fraud.detector.rule.CardTestingRule;
import dev.thiagogomes.fraud.detector.rule.HighAmountRule;
import dev.thiagogomes.fraud.detector.rule.ImpossibleTravelRule;
import dev.thiagogomes.fraud.detector.transaction.Transaction;

@Configuration(proxyBeanMethods = false)
public class FraudTopology {

    public static final String TRANSACTIONS_TOPIC = "transactions";
    public static final String ALERTS_TOPIC = "fraud-alerts";
    public static final String TRANSACTIONS_DLT = "transactions-dlt";
    public static final String ALERTS_DLT = "fraud-alerts-dlt";

    @Bean
    public KStream<String, FraudAlert> fraudAlerts(StreamsBuilder builder, FraudRulesProperties properties,
                                                  Clock clock) {
        var transactionSerde = Serdes.serdeFrom(new JacksonJsonSerializer<Transaction>()
                .noTypeInfo(), new TransactionDeserializer(clock));
        var alertSerde = new JacksonJsonSerde<>(FraudAlert.class).noTypeInfo().ignoreTypeHeaders();

        KStream<String, Transaction> transactions = builder.stream(TRANSACTIONS_TOPIC,
                Consumed.with(Serdes.String(), transactionSerde)
                        .withTimestampExtractor(new TransactionTimestampExtractor()));
        transactions = transactions.filter((key, value) -> value != null);
        transactions.filter((key, value) -> !value.cardId().equals(key))
                .to(TRANSACTIONS_DLT, Produced.with(Serdes.String(), transactionSerde));
        transactions = transactions.filter((key, value) -> value.cardId().equals(key));
        KStream<String, FraudAlert> alerts = new VelocityRule(properties.velocity(), clock).apply(builder, transactions)
                .merge(new HighAmountRule(properties.highAmount(), clock).apply(builder, transactions))
                .merge(new ImpossibleTravelRule(properties.impossibleTravel(), clock).apply(builder, transactions))
                .merge(new CardTestingRule(properties.cardTesting(), clock).apply(builder, transactions));
        alerts.to(ALERTS_TOPIC, Produced.with(Serdes.String(), alertSerde));
        return alerts;
    }
}
