package com.rhysmccaig.kscheduler.streams;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.serdes.ScheduledRecordMetadataSerializer;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class SourceKeyDefaultStreamPartitioner implements StreamPartitioner<ScheduledRecordMetadata, ScheduledRecord> {

    final ScheduledRecordMetadataSerializer serializer = new ScheduledRecordMetadataSerializer();

    @Override
    public Integer partition(final String topic, final ScheduledRecordMetadata key, final ScheduledRecord value, final int numPartitions) {
        Integer partition = null;
        if (value != null && value.key() != null) {
            partition = Utils.toPositive(Utils.murmur2(value.key())) % numPartitions;
        }
        return partition;
    }

}