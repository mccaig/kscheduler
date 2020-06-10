package com.rhysmccaig.kscheduler;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Properties;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.util.HeaderUtils;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KSchedulerTest {
  
  private static String INPUT_TOPIC = "inputtopic";
  private static Duration ONE_MINUTE = Duration.ofMinutes(1);
  private static String OUTPUT_TOPIC_A = "outputtopica";
  private static String OUTPUT_TOPIC_B = "outputtopicb";
  private static String OUTPUT_TOPIC_UNKNOWN = "output.topic.unknown";

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<Bytes, Bytes> outputTopicA;
  private TestOutputTopic<Bytes, Bytes> outputTopicB;
  private KeyValueStore<ScheduledId, ScheduledRecord> kvStore;
  private Clock clock;
  private final Instant recordBaseTime = Instant.EPOCH; 

  @BeforeEach
  public void setup() {
    var storeBuilder = KScheduler.getStoreBuilder();
    var topology = KScheduler.getTopology(INPUT_TOPIC, ONE_MINUTE, storeBuilder);
    // setup test driver
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    testDriver = new TopologyTestDriver(topology, props);
    inputTopic = testDriver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());
    kvStore = testDriver.getKeyValueStore(storeBuilder.name());
    outputTopicA = testDriver.createOutputTopic(OUTPUT_TOPIC_A, new BytesDeserializer(), new BytesDeserializer());
    clock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());
    //outputTopicB = testDriver.createOutputTopic(OUTPUT_TOPIC_B, new BytesDeserializer(), new BytesDeserializer());
  }

  @Test
  public void test() {
    var testRecord = new TestRecord<String, String>(
        "Hello", 
        "World", 
        new RecordHeaders().add(new RecordHeader("key", "value".getBytes(StandardCharsets.UTF_8))),
        Instant.now(clock));
    inputTopic.pipeInput(testRecord);
    assertFalse(kvStore.all().hasNext());   
    //testDriver.readOutput(OUTPUT_TOPIC_A).
    //Read value and validate it, ignore validation of kafka key, timestamp is irrelevant in this case
    //assert(outputTopicA.readValue() != null);
    //No more output in topic
    //assert(outputTopicA.isEmpty());

  }
}