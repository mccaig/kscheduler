package com.rhysmccaig.kscheduler;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.serialization.ScheduledRecordMetadataSerde;
import com.rhysmccaig.kscheduler.streams.SchedulerTransformer;
import com.rhysmccaig.kscheduler.util.HeaderUtils;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KSchedulerTest {
  
  private static Serde<ScheduledRecordMetadata> METADATA_SERDE = new ScheduledRecordMetadataSerde();
  private static Serializer<ScheduledRecordMetadata> METADATA_SERIALIZER = METADATA_SERDE.serializer();

  private static Duration PUNCTUATE_DURATION = Duration.ofSeconds(1);
  private static Duration MAX_DELAY = Duration.ofHours(1);

  private static String INPUT_TOPIC = "inputtopic";
  private static String SCHEDULED_TOPIC = "scheduledtopic";
  private static String OUTGOING_TOPIC = "outgoingtopic";
  private static Duration ONE_MINUTE = Duration.ofMinutes(1);
  private static String OUTPUT_TOPIC_A = "output.topic.a";
  private static String OUTPUT_TOPIC_B = "output.topic.b";

  private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");

  private TopologyTestDriver testDriver;
  private TestInputTopic<byte[], byte[]> inputTopic;
  private TestOutputTopic<byte[], byte[]> outputTopicA;
  private TestOutputTopic<byte[], byte[]> outputTopicB;
  private KeyValueStore<ScheduledId, ScheduledRecord> scheduledRecordStore;

  private ScheduledRecordMetadata metadataScheduledIn1Min;
  
  /**
   * Test Setup.
   */
  @BeforeEach
  public void setup() {
    var scheduledRecordStoreBuilder = SchedulerTransformer.getScheduledRecordStoreBuilder();
    var scheduledIdStoreBuilder = SchedulerTransformer.getScheduledIdStoreBuilder();
    var topology = KScheduler.getTopology(
        INPUT_TOPIC, SCHEDULED_TOPIC, OUTGOING_TOPIC, 
        scheduledRecordStoreBuilder, scheduledIdStoreBuilder, PUNCTUATE_DURATION, MAX_DELAY);
    // As our topology routes output records dynamically, 
    // the topics we are testing are not initialized in TopologyTestDriver
    // Unless we create a dummy sub-topology and add them to it
    topology.addSource("DUMMY_SOURCE", "dummy");
    topology.addSink("DUMMY_OUTPUT_A", OUTPUT_TOPIC_A, "DUMMY_SOURCE");
    topology.addSink("DUMMY_OUTPUT_B", OUTPUT_TOPIC_B, "DUMMY_SOURCE");
    // setup test driver
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    testDriver = new TopologyTestDriver(topology, props, Instant.EPOCH);
    inputTopic = testDriver.createInputTopic(INPUT_TOPIC, new ByteArraySerializer(), new ByteArraySerializer());
    scheduledRecordStore = testDriver.getKeyValueStore(scheduledRecordStoreBuilder.name());
    outputTopicA = 
        testDriver.createOutputTopic(OUTPUT_TOPIC_A, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    outputTopicB = 
        testDriver.createOutputTopic(OUTPUT_TOPIC_B, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    metadataScheduledIn1Min =
        new ScheduledRecordMetadata(Instant.EPOCH.plus(ONE_MINUTE), Instant.MAX, Instant.MIN, ID, OUTPUT_TOPIC_A);
  }

  @AfterEach
  public void tearDown() {
    testDriver.close();
  }

  @Test
  public void recordWithNoMetadataIsDropped() {
    var key = "Hello".getBytes(UTF_8);
    var value = "World!".getBytes(UTF_8);
    var testRecord = new TestRecord<byte[], byte[]>(
        key, 
        value, 
        new RecordHeaders().add(new RecordHeader("Header", "HeaderValue".getBytes(UTF_8))),
        Instant.EPOCH);
    inputTopic.pipeInput(testRecord);
    assertFalse(scheduledRecordStore.all().hasNext());  
    assertTrue(outputTopicA.isEmpty());
  }

  @Test
  public void recordIsForwardedAtScheduledTime() {
    var key = "Hello".getBytes(UTF_8);
    var value = "World!".getBytes(UTF_8);
    var metadataBytes = METADATA_SERIALIZER.serialize(null, metadataScheduledIn1Min);
    var headerKey = "HeaderKey";
    var headerValue = "HeaderValue".getBytes(UTF_8);
    var header = new RecordHeader(headerKey, headerValue);
    var headers = new RecordHeaders();
    headers.add(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY, metadataBytes);
    headers.add(header);
    var initialRecord = new TestRecord<byte[], byte[]>(key, value, headers, Instant.EPOCH);
    
    // Send the record
    inputTopic.pipeInput(initialRecord);
    // No record immediately propogated downstream
    assertTrue(outputTopicA.isEmpty());
    
    // Advance the clock 30 seconds.
    // No records should be propogated to output topics
    testDriver.advanceWallClockTime(Duration.ofSeconds(30));
    assertTrue(outputTopicA.isEmpty());
    
    // Advance the clock another 30 seconds - record is scheduled for this time
    // Expect the record to be propogated downstream
    // Output record should have 2 headers - one for the destination, and the other existing header
    testDriver.advanceWallClockTime(Duration.ofSeconds(30));
    assertFalse(outputTopicA.isEmpty());
    var outputRecord = outputTopicA.readRecord();
    assertArrayEquals(key, outputRecord.getKey());
    assertArrayEquals(value, outputRecord.getValue());
    assertEquals(2, outputRecord.headers().toArray().length);
    assertEquals(header, outputRecord.headers().lastHeader(headerKey));
    var destinationHeader = outputRecord.headers().lastHeader(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY);
    var expectedDestinationHeader = new RecordHeader(
        HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY, 
        OUTPUT_TOPIC_A.getBytes(UTF_8));
    assertEquals(expectedDestinationHeader, destinationHeader);
  }

  @Test
  public void recordWithExcessiveScheduledTimeIsDropped() {
    var key = "Hello".getBytes(UTF_8);
    var value = "World!".getBytes(UTF_8);
    var metadataScheduledIn2Hours = new ScheduledRecordMetadata(
          Instant.EPOCH.plus(Duration.ofHours(2)), Instant.MAX, Instant.MIN, ID, OUTPUT_TOPIC_A);
    var metadataBytes = METADATA_SERIALIZER.serialize(null, metadataScheduledIn2Hours);
    var headers = new RecordHeaders();
    headers.add(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY, metadataBytes);
    var record = new TestRecord<byte[], byte[]>(key, value, headers, Instant.EPOCH);
    // Send the record
    inputTopic.pipeInput(record);
    // No record immediately propogated downstream
    assertTrue(outputTopicA.isEmpty());
    // Advance the clock 1 hour.
    // No records should be propogated to output topics
    testDriver.advanceWallClockTime(Duration.ofHours(1));
    assertTrue(outputTopicA.isEmpty());
    // Advance the clock 2 hours.
    // No records should be propogated to output topics
    testDriver.advanceWallClockTime(Duration.ofHours(2));
    assertTrue(outputTopicA.isEmpty());

  }

  @Test
  public void updatedRecordIsForwardedToUpdatedTopic() {
    var key = "Hello".getBytes(UTF_8);
    var value = "World!".getBytes(UTF_8);
    var metadataBytes = METADATA_SERIALIZER.serialize(null, metadataScheduledIn1Min);
    var headers = new RecordHeaders();
    headers.add(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY, metadataBytes);
    var initialRecord = new TestRecord<byte[], byte[]>(key, value, headers, Instant.EPOCH);
    var metadataScheduledToNewTopic = new ScheduledRecordMetadata(
          Instant.EPOCH.plus(ONE_MINUTE), Instant.MAX, Instant.MIN, ID, OUTPUT_TOPIC_B);
    var updateHeaders = new RecordHeaders();
    updateHeaders.add(
          HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY, METADATA_SERIALIZER.serialize(null, metadataScheduledToNewTopic));
    inputTopic.pipeInput(initialRecord);
    // No record propogated downstream
    assertTrue(outputTopicA.isEmpty());
    // Advance the clock 30 seconds.
    // Record should still be in the store and no records should be propogated to output topics
    testDriver.advanceWallClockTime(Duration.ofSeconds(30));
    // Send the new record
    var updateRecord = 
        new TestRecord<byte[], byte[]>(key, value, updateHeaders, Instant.EPOCH.plus(Duration.ofSeconds(30)));
    inputTopic.pipeInput(updateRecord);
    assertTrue(outputTopicA.isEmpty());
    assertTrue(outputTopicB.isEmpty());
    // Advance the clock another 30 seconds - record is scheduled for this time, but should go to the updated topic
    // Expect the no record to be propogated
    testDriver.advanceWallClockTime(Duration.ofSeconds(30));
    assertTrue(outputTopicA.isEmpty());
    assertFalse(outputTopicB.isEmpty());
    var outputRecord = outputTopicB.readRecord();
    assertArrayEquals(key, outputRecord.getKey());
    assertArrayEquals(value, outputRecord.getValue());
    assertEquals(1, outputRecord.headers().toArray().length);
    var destinationHeader = outputRecord.headers().lastHeader(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY);
    var expectedDestinationHeader = 
        new RecordHeader(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY, OUTPUT_TOPIC_B.getBytes(UTF_8));
    assertEquals(expectedDestinationHeader, destinationHeader);
  }



  @Test
  public void deletedRecordIsNotForwarded() {
    var key = "Hello".getBytes(UTF_8);
    var value = "World!".getBytes(UTF_8);
    var metadataBytes = METADATA_SERIALIZER.serialize(null, metadataScheduledIn1Min);
    var headers = new RecordHeaders();
    headers.add(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY, metadataBytes);
    var initialRecord = new TestRecord<byte[], byte[]>(key, value, headers, Instant.EPOCH);
    var metadataScheduledAtMinInstant = 
        new ScheduledRecordMetadata(Instant.EPOCH.plus(ONE_MINUTE), Instant.MIN, Instant.MIN, ID, OUTPUT_TOPIC_A);
    var deleteHeaders = new RecordHeaders();
    deleteHeaders.add(
        HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY, 
        METADATA_SERIALIZER.serialize(null, metadataScheduledAtMinInstant));
    inputTopic.pipeInput(initialRecord);
    // No record propogated downstream
    assertTrue(outputTopicA.isEmpty());
    // Advance the clock 30 seconds.
    // Record should still be in the store and no records should be propogated to output topics
    testDriver.advanceWallClockTime(Duration.ofSeconds(30));
    // Send the new record
    var deleteRecord = 
        new TestRecord<byte[], byte[]>(key, value, deleteHeaders, Instant.EPOCH.plus(Duration.ofSeconds(30)));
    inputTopic.pipeInput(deleteRecord);
    assertTrue(outputTopicA.isEmpty());
    // Advance the clock another 30 seconds - original record is scheduled for this time
    // Expect the no record to be propogated
    testDriver.advanceWallClockTime(Duration.ofSeconds(30));
    assertTrue(outputTopicA.isEmpty());
    // And the same after one minute
    testDriver.advanceWallClockTime(Duration.ofMinutes(1));
    assertTrue(outputTopicA.isEmpty());
  }

}