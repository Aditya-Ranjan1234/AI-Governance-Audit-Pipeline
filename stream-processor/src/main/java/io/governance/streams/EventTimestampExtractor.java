package io.governance.streams;

import io.governance.model.GovernanceEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public final class EventTimestampExtractor implements TimestampExtractor {
    @Override public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof GovernanceEvent event && event.eventTime() != null) {
            return event.eventTime().toEpochMilli();
        }
        return record.timestamp() >= 0 ? record.timestamp() : partitionTime;
    }
}

