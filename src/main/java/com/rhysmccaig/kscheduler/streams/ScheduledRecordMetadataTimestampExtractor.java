// package com.rhysmccaig.kscheduler.streams;

// import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;

// import org.apache.kafka.clients.consumer.ConsumerRecord;
// import org.apache.kafka.streams.processor.TimestampExtractor;

// public class ScheduledRecordMetadataTimestampExtractor implements TimestampExtractor{
  
//   @Override
//   public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
//     long timestamp = -1;
//     final ScheduledRecordMetadata metadata;
//     try {
//       metadata = (ScheduledRecordMetadata) record.key();
//       if (metadata != null && metadata.produced() != null) {
//         timestamp = metadata.produced().getEpochSecond();
//       }
//     } catch (ClassCastException ex) {}
//     return timestamp;
//   }

// }