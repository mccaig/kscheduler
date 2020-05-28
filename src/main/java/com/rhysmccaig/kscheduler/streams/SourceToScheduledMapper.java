package com.rhysmccaig.kscheduler.streams;

import java.util.List;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class SourceToScheduledMapper implements KeyValueMapper<byte[], byte[], Iterable<KeyValue<ScheduledRecordMetadata, ScheduledRecord>>> {
    
    @Override
    public Iterable<KeyValue<ScheduledRecordMetadata, ScheduledRecord>> apply(byte[] key, byte[] value) {
        //var metadata = new ScheduledRecordMetadata(, id, destination, created, expires, produced, error)
        return List.of(new KeyValue<ScheduledRecordMetadata, ScheduledRecord>(
            new ScheduledRecordMetadata(null, null, null, null, null, null, null),
            new ScheduledRecord(null, null, null)));
    }
}