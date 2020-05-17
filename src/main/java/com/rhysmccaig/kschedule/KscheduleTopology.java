// package com.rhysmccaig.kschedule;

// import static java.util.Map.entry;

// import java.util.Map;

// import org.apache.kafka.common.serialization.Serdes;
// import org.apache.kafka.streams.StreamsBuilder;
// import org.apache.kafka.streams.Topology;
// import org.apache.kafka.streams.kstream.Consumed;

// public class KscheduleTopology {

//   static final String INPUT_TOPIC = "input";
//   static final String OUTPUT_TOPIC = "output";
//   static final Map<Integer,String> WAIT_TOPICS = Map.ofEntries(
//     entry(1, "w1"),
//     entry(2, "w2"),
//     entry(3, "w3"),
//     entry(5, "w5"),
//     entry(10, "w10"),
//     entry(15, "w15"),
//     entry(30, "w30"),
//     entry(60, "w60"),
//     entry(120, "w120"),
//     entry(180, "w180"),
//     entry(360, "w360"),
//     entry(720, "w720"),
//     entry(1440, "w1440")
//   );

//   public Topology buildTopology() {
//     StreamsBuilder builder = new StreamsBuilder();

//     builder
//       .stream(INPUT_TOPIC, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
//       .to(OUTPUT_TOPIC);

//       return builder.build();
//   }


// }