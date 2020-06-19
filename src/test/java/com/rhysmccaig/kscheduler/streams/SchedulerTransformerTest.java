// package com.rhysmccaig.kscheduler.streams;

// import static org.junit.jupiter.api.Assertions.assertArrayEquals;
// import static org.junit.jupiter.api.Assertions.assertEquals;
// import static org.junit.jupiter.api.Assertions.assertFalse;
// import static org.junit.jupiter.api.Assertions.assertTrue;

// import java.nio.charset.StandardCharsets;
// import java.time.Clock;
// import java.time.Duration;
// import java.time.Instant;
// import java.time.ZoneId;
// import java.util.List;
// import java.util.Properties;
// import java.util.UUID;

// import com.rhysmccaig.kscheduler.model.ScheduledId;
// import com.rhysmccaig.kscheduler.model.ScheduledRecord;
// import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
// import com.rhysmccaig.kscheduler.serialization.ScheduledIdSerde;
// import com.rhysmccaig.kscheduler.serialization.ScheduledRecordMetadataSerde;
// import com.rhysmccaig.kscheduler.serialization.ScheduledRecordMetadataSerializer;
// import com.rhysmccaig.kscheduler.serialization.ScheduledRecordSerde;
// import com.rhysmccaig.kscheduler.util.HeaderUtils;

// import org.apache.kafka.common.header.internals.RecordHeader;
// import org.apache.kafka.common.header.internals.RecordHeaders;
// import org.apache.kafka.streams.StreamsBuilder;
// import org.apache.kafka.streams.StreamsConfig;
// import org.apache.kafka.streams.TestInputTopic;
// import org.apache.kafka.streams.TestOutputTopic;
// import org.apache.kafka.streams.TopologyTestDriver;
// import org.apache.kafka.streams.kstream.Consumed;
// import org.apache.kafka.streams.kstream.Named;
// import org.apache.kafka.streams.kstream.Produced;
// import org.apache.kafka.streams.state.KeyValueStore;
// import org.apache.kafka.streams.state.Stores;
// import org.apache.kafka.streams.test.TestRecord;
// import org.junit.jupiter.api.AfterEach;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Test;

// public class SchedulerTransformerTest {
  
//   private static Instant SCHEDULED = Instant.EPOCH;
//   private static Instant EXPIRES = Instant.MAX;
//   private static Instant CREATED = Instant.MIN;
//   private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");
//   private static String DESTINATION = "topic";
//   private static ScheduledRecordMetadata METADATA = new ScheduledRecordMetadata(SCHEDULED, EXPIRES, CREATED, ID, DESTINATION);
  
//   private static ScheduledRecordMetadataSerde METADATA_SERDE = new ScheduledRecordMetadataSerde();
//   private static ScheduledRecordSerde RECORD_SERDE = new ScheduledRecordSerde();

//   private static String STATE_STORE_NAME = SchedulerTransformer.DEFAULT_STATE_STORE_NAME;
//   private static Duration PUNCTUATE_SCHEDULE = Duration.ofSeconds(1);
//   private static String IN_TOPIC_NAME = "in.topic";
//   private static String OUT_TOPIC_NAME = "out.topic";
//   private static Instant TEST_START_TIME = Instant.EPOCH;

//   private TopologyTestDriver testDriver;
//   private TestInputTopic<ScheduledRecordMetadata, ScheduledRecord> inTopic;
//   private TestOutputTopic<ScheduledRecordMetadata, ScheduledRecord> outTopic;
//   private KeyValueStore<ScheduledId, ScheduledRecord> kvStore;

//   private SchedulerTransformer transformer;

//   @BeforeEach
//   public void setup() {
//     // Simple topology for testing
//     var storeBuilder = Stores.keyValueStoreBuilder(
//         Stores.inMemoryKeyValueStore(STATE_STORE_NAME),
//         new ScheduledIdSerde(),
//         new ScheduledRecordSerde())
//       .withLoggingDisabled();
//     var streamsBuilder = new StreamsBuilder();
//     streamsBuilder.addStateStore(storeBuilder)
//         .stream(IN_TOPIC_NAME, Consumed.with(METADATA_SERDE, RECORD_SERDE))
//         .transform(() -> new SchedulerTransformer(PUNCTUATE_SCHEDULE), Named.as("SCHEDULER"), STATE_STORE_NAME)
//         .to(OUT_TOPIC_NAME, Produced.with(METADATA_SERDE, RECORD_SERDE));
//     var topology = streamsBuilder.build();
//     // setup test driver
//     Properties props = new Properties();
//     props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kscheduler-unit-test");
//     props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
//     testDriver = new TopologyTestDriver(topology, props, TEST_START_TIME);
//     inTopic = testDriver.createInputTopic(IN_TOPIC_NAME, METADATA_SERDE.serializer(), RECORD_SERDE.serializer());
//     kvStore = testDriver.getKeyValueStore(storeBuilder.name());
//     outTopic = testDriver.createOutputTopic(OUT_TOPIC_NAME, METADATA_SERDE.deserializer(), RECORD_SERDE.deserializer());
//   }

//   @AfterEach
//   public void teardown() {
//     testDriver.close();
//   }

//   @Test
//   public void recordIsForwardedAtScheduledTime() {
//     // Record scheduled in one minute
//     var scheduled = Instant.EPOCH.plus(Duration.ofMinutes(1));
//     var expires = Instant.MAX;
//     var created = Instant.MIN;
//     var id = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");
//     var destination = "destination.topic";
//     var metadata = new ScheduledRecordMetadata(scheduled, expires, created, id, destination);
//     // Record
//     var sourceKey = "Hello".getBytes(StandardCharsets.UTF_8);
//     var sourceValue = "World!".getBytes(StandardCharsets.UTF_8);
//     var headerKey = "HeaderKey";
//     var headerValue = "HeaderValue".getBytes(StandardCharsets.UTF_8);
//     var header = new RecordHeader(headerKey, headerValue);
//     var headers = new RecordHeaders(List.of(header));
//     var record = new ScheduledRecord(metadata, sourceKey, sourceValue, headers);
//     var testRecord = new TestRecord<ScheduledRecordMetadata, ScheduledRecord>(metadata, record, headers, TEST_START_TIME);
    
//     inTopic.pipeInput(testRecord);
//     // Expect one record to be in the kvStore after the first record is input
//     // No record propogated downstream
//     // Stored Record headers should not include KScheduler Headers
//     var kvStoreIt = kvStore.all();
//     var countEntries = 0;
//     while(kvStoreIt.hasNext()) {
//       countEntries++;
//       kvStoreIt.next();
//     }
//     assertEquals(1, countEntries);
//     var kvRecord = kvStore.all().next();
//     assertEquals(metadata.id(), kvRecord.key.id());
//     assertEquals(metadata.scheduled(), kvRecord.key.scheduled());
//     assertEquals(metadata, kvRecord.value.metadata());
//     assertArrayEquals(sourceKey, kvRecord.value.key());
//     assertArrayEquals(sourceValue, kvRecord.value.value());
//     assertEquals(headers, kvRecord.value.headers());
//     assertTrue(outTopic.isEmpty());
//     // Advance the clock 30 seconds.
//     // Record should still be in the store and no records should be propogated to output topics
//     testDriver.advanceWallClockTime(Duration.ofSeconds(30));
//     kvStoreIt = kvStore.all();
//     countEntries = 0;
//     while(kvStoreIt.hasNext()) {
//       countEntries++;
//       kvStoreIt.next();
//     }
//     assertEquals(1, countEntries);
//     //assertTrue(outputTopicA.isEmpty());
//     // Advance the clock another 30 seconds - record is scheduled for this time
//     // Expect the record to be removed from the kvStore and propogated downstream
//     // Output record should have 2 headers - one for the destination, and the other existing header
//     testDriver.advanceWallClockTime(Duration.ofSeconds(30));
//     assertFalse(kvStore.all().hasNext());
//     assertFalse(outTopic.isEmpty());
//     var outputRecord = outTopic.readRecord();
//     assertEquals(metadata, outputRecord.getKey());
//     assertEquals(record, outputRecord.getValue());
//   }

//   @Test
//   public void existingScheduledRecordIsForwardedAtScheduledTime() {
//     // Record scheduled in one minute
//     var scheduled = Instant.EPOCH.plus(Duration.ofMinutes(1));
//     var expires = Instant.EPOCH;
//     var created = Instant.MIN;
//     var id = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");
//     var destination = "destination.topic";
//     var metadata = new ScheduledRecordMetadata(scheduled, expires, created, id, destination);
//     // Record
//     var sourceKey = "Hello".getBytes(StandardCharsets.UTF_8);
//     var sourceValue = "World!".getBytes(StandardCharsets.UTF_8);
//     var record = new ScheduledRecord(metadata, sourceKey, sourceValue, null);
//     var scheduledId = new ScheduledId(scheduled, id);
//     // Pre populate the store
//     kvStore.put(scheduledId, record);
//     // Expect that there is a record in the kvStore
//     var kvStoreIt = kvStore.all();
//     var countEntries = 0;
//     while(kvStoreIt.hasNext()) {
//       countEntries++;
//       kvStoreIt.next();
//     }
//     assertEquals(1, countEntries);
//     assertTrue(outTopic.isEmpty());
//     // record should remain in the store after 30 seconds
//     testDriver.advanceWallClockTime(Duration.ofSeconds(30));
//     kvStoreIt = kvStore.all();
//     countEntries = 0;
//     while(kvStoreIt.hasNext()) {
//       countEntries++;
//       kvStoreIt.next();
//     }
//     assertEquals(1, countEntries);
//     assertTrue(outTopic.isEmpty());
//     // Record should be propogated downstream after another 30 seconds (Instant.EPOCH.plus(Duration.ofMinutes(1)))
//     testDriver.advanceWallClockTime(Duration.ofSeconds(30));
//     kvStoreIt = kvStore.all();
//     assertFalse(kvStoreIt.hasNext());
//     assertFalse(outTopic.isEmpty());
//     var outputRecord = outTopic.readRecord();
//     assertEquals(metadata, outputRecord.getKey());
//     assertEquals(record, outputRecord.getValue());
//   }

//   @Test
//   public void expiredRecordIsDropped() {
//     // Record scheduled in one minute
//     var scheduled = Instant.EPOCH.plus(Duration.ofMinutes(1));
//     var expires = Instant.EPOCH;
//     var created = Instant.MIN;
//     var id = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");
//     var destination = "destination.topic";
//     var metadata = new ScheduledRecordMetadata(scheduled, expires, created, id, destination);
//     // Record
//     var sourceKey = "Hello".getBytes(StandardCharsets.UTF_8);
//     var sourceValue = "World!".getBytes(StandardCharsets.UTF_8);
//     var record = new ScheduledRecord(metadata, sourceKey, sourceValue, null);
//     var testRecord = new TestRecord<ScheduledRecordMetadata, ScheduledRecord>(metadata, record, null, TEST_START_TIME);
//     // Test
//     inTopic.pipeInput(testRecord);
//     // Expect that there is no record persisted in the KV store and no record propogated downstream, its just dropped.
//     assertFalse(kvStore.all().hasNext());
//     assertTrue(outTopic.isEmpty());
//   }

//   @Test
//   public void existingIdShouldBeUpdatedWithNewMetadataAndRecord() {
//     // Record scheduled in one minute
//     var initialScheduled = Instant.EPOCH.plus(Duration.ofMinutes(5));
//     var updatedScheduled = Instant.EPOCH.plus(Duration.ofMinutes(1));
//     var expires = Instant.EPOCH;
//     var created = Instant.MIN;
//     var id = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");
//     var destination = "destination.topic";
//     var initialMetadata = new ScheduledRecordMetadata(initialScheduled, expires, created, id, destination);
//     var updatedMetadata = new ScheduledRecordMetadata(updatedScheduled, expires, created, id, destination);
//     // Records
//     var sourceKey = "Hello".getBytes(StandardCharsets.UTF_8);
//     var initialSourceValue = "World!".getBytes(StandardCharsets.UTF_8);
//     var updatedSourceValue = "Universe!".getBytes(StandardCharsets.UTF_8);
//     var initialRecord = new ScheduledRecord(initialMetadata, sourceKey, initialSourceValue, null);
//     var updatedRecord = new ScheduledRecord(updatedMetadata, sourceKey, updatedSourceValue, null);
//     var initialScheduledId = new ScheduledId(initialScheduled, id);
//     // Add the initial record into the store
//     kvStore.put(initialScheduledId, initialRecord);
//     // Updated record to send to topology
//     var testRecord = new TestRecord<ScheduledRecordMetadata, ScheduledRecord>(updatedMetadata, updatedRecord, null, TEST_START_TIME);
//     // Test
//     var kvStoreIt = kvStore.all();
//     var countEntries = 0;
//     while(kvStoreIt.hasNext()) {
//       countEntries++;
//       kvStoreIt.next();
//     }
//     assertEquals(1, countEntries);
//     assertTrue(outTopic.isEmpty());
//     inTopic.pipeInput(testRecord);
//     // Expect that there is one record persisted in the KV store and no record propogated downstream
//     // new record should be in the store
//     kvStoreIt = kvStore.all();
//     countEntries = 0;
//     while(kvStoreIt.hasNext()) {
//       countEntries++;
//       kvStoreIt.next();
//     }
//     assertEquals(1, countEntries);
//     assertTrue(outTopic.isEmpty());
//     // After one minute the new record should be propogated downstream
//     testDriver.advanceWallClockTime(Duration.ofMinutes(1));
//     kvStoreIt = kvStore.all();
//     assertFalse(kvStoreIt.hasNext());
//     assertFalse(outTopic.isEmpty());
//     var outputRecord = outTopic.readRecord();
//     assertEquals(updatedMetadata, outputRecord.getKey());
//     assertEquals(updatedRecord, outputRecord.getValue());
//   }



// }