package dev.thiagogomes.fraud.detector.stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TimestampExtractor;

import dev.thiagogomes.fraud.detector.transaction.Transaction;

public class TransactionTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() == null) {
            return Math.max(0, record.timestamp());
        }
        if (!(record.value() instanceof Transaction transaction) || transaction.timestamp() == null
                || transaction.timestamp().toEpochMilli() < 0) {
            throw new StreamsException("Transação sem event time válido");
        }
        return transaction.timestamp().toEpochMilli();
    }
}
