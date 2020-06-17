package com.rhysmccaig.kscheduler;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Properties;
import java.util.UUID;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.serialization.ScheduledRecordMetadataSerde;
import com.rhysmccaig.kscheduler.streams.SchedulerTransformer;
import com.rhysmccaig.kscheduler.util.HeaderUtils;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
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
  private static Deserializer<ScheduledRecordMetadata> METADATA_DESERIALIZER = METADATA_SERDE.deserializer();


  private static String INPUT_TOPIC = "inputtopic";
  private static String SCHEDULED_TOPIC = "scheduledtopic";
  private static String OUTGOING_TOPIC = "outgoingtopic";
  private static Duration ONE_MINUTE = Duration.ofMinutes(1);
  private static String OUTPUT_TOPIC_A = "output.topic.a";
  private static String OUTPUT_TOPIC_B = "output.topic.b";
  private static String OUTPUT_TOPIC_UNKNOWN = "output.topic.unknown";

  private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");

  private TopologyTestDriver testDriver;
  private TestInputTopic<byte[], byte[]> inputTopic;
  private TestOutputTopic<byte[], byte[]> outputTopicA;
  private TestOutputTopic<byte[], byte[]> outputTopicB;
  private KeyValueStore<ScheduledId, ScheduledRecord> kvStore;
  private Clock clock;
  private final Instant recordBaseTime = Instant.EPOCH;
  private Instant now; 

  private ScheduledRecordMetadata metadataScheduledIn1Min;
  

  @BeforeEach
  public void setup() {
    var storeBuilder = KScheduler.getStoreBuilder();
    var topology = KScheduler.getTopology(INPUT_TOPIC, SCHEDULED_TOPIC, OUTGOING_TOPIC, ONE_MINUTE, storeBuilder);
    topology.addSource("DUMMY_SOURCE", "dummy");
    topology.addSink("DUMMY_OUTPUT_A", OUTPUT_TOPIC_A, "DUMMY_SOURCE");
    topology.addSink("DUMMY_OUTPUT_B", OUTPUT_TOPIC_B, "DUMMY_SOURCE");
    // setup test driver
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    testDriver = new TopologyTestDriver(topology, props);
    inputTopic = testDriver.createInputTopic(INPUT_TOPIC, new ByteArraySerializer(), new ByteArraySerializer());
    kvStore = testDriver.getKeyValueStore(storeBuilder.name());
    outputTopicA = testDriver.createOutputTopic(OUTPUT_TOPIC_A, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    outputTopicB = testDriver.createOutputTopic(OUTPUT_TOPIC_B, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    clock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());
    now = Instant.now(clock);
    metadataScheduledIn1Min = new ScheduledRecordMetadata(now.plus(ONE_MINUTE), Instant.MAX, Instant.MIN, ID, OUTPUT_TOPIC_A);
  }

  @AfterEach
  public void tearDown() {
    testDriver.close();
  }

  @Test
  public void recordWithNoMetadataIsDropped() {
    var key = "Hello".getBytes(StandardCharsets.UTF_8);
    var value = "World!".getBytes(StandardCharsets.UTF_8);
    var testRecord = new TestRecord<byte[], byte[]>(
        key, 
        value, 
        new RecordHeaders().add(new RecordHeader("Header", "HeaderValue".getBytes(StandardCharsets.UTF_8))),
        now);
    inputTopic.pipeInput(testRecord);
    var it = kvStore.all();
    while (it.hasNext()) {
      System.out.println(it.next());
    }
    assertFalse(kvStore.all().hasNext());   
    //Read value and validate it, ignore validation of kafka key, timestamp is irrelevant in this case
    assert(outputTopicA.readValue() != null);
    //No more output in topic
    assert(outputTopicA.isEmpty());
  }

  @Test
  public void recordIsForwardedAtScheduledTime() {
    var key = "Hello".getBytes(StandardCharsets.UTF_8);
    var value = "World!".getBytes(StandardCharsets.UTF_8);
    var headerKey = "HeaderKey";
    var headerValue = "HeaderValue".getBytes(StandardCharsets.UTF_8);
    var otherHeader = new RecordHeader(headerKey, headerValue);
    var expectedHeaders = new RecordHeaders();
    expectedHeaders.add(otherHeader);
    var metadataBytes = METADATA_SERIALIZER.serialize(null, metadataScheduledIn1Min);
    var headers = new RecordHeaders();
    headers.add(otherHeader);
    headers.add(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY, metadataBytes);
    var testRecord = new TestRecord<byte[], byte[]>(key, value, headers, now);
    inputTopic.pipeInput(testRecord);
    // Expect one record to be in the kvStore after the first record is input
    // No record propogated downstream
    // Stored Record headers should not include KScheduler Headers
    var kvStoreIt = kvStore.all();
    var countEntries = 0;
    while(kvStoreIt.hasNext()) {
      countEntries++;
      kvStoreIt.next();
    }
    assertEquals(1, countEntries);
    var kvRecord = kvStore.all().next();
    assertEquals(metadataScheduledIn1Min.id(), kvRecord.key.id());
    assertEquals(metadataScheduledIn1Min.scheduled(), kvRecord.key.scheduled());
    assertEquals(metadataScheduledIn1Min, kvRecord.value.metadata());
    assertArrayEquals(key, kvRecord.value.key());
    assertArrayEquals(value, kvRecord.value.value());
    assertEquals(expectedHeaders, kvRecord.value.headers());
    // assertTrue(outputTopicA.isEmpty());
    // Advance the clock 30 seconds.
    // Record should still be in the store and no records should be propogated to output topics
    testDriver.advanceWallClockTime(Duration.ofSeconds(30));
    kvStoreIt = kvStore.all();
    countEntries = 0;
    while(kvStoreIt.hasNext()) {
      countEntries++;
      kvStoreIt.next();
    }
    assertEquals(1, countEntries);
    //assertTrue(outputTopicA.isEmpty());
    // Advance the clock another 30 seconds - record is scheduled for this time
    // Expect the record to be removed from the kvStore and propogated downstream
    // Output record should have 2 headers - one for the destination, and the other existing header
    testDriver.advanceWallClockTime(Duration.ofSeconds(90));
    assertFalse(kvStore.all().hasNext());
    assertFalse(outputTopicA.isEmpty());
    var outputRecord = outputTopicA.readRecord();
    assertArrayEquals(key, outputRecord.getKey());
    assertArrayEquals(value, outputRecord.getValue());
    assertEquals(2, outputRecord.headers().toArray().length);
    assertEquals(otherHeader, outputRecord.headers().lastHeader(headerKey));
    var destinationHeader = outputRecord.headers().lastHeader(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY);
    var expectedDestinationHeader = new RecordHeader(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY, OUTPUT_TOPIC_A.getBytes(StandardCharsets.UTF_8));
    assertEquals(expectedDestinationHeader, destinationHeader);
  }

}