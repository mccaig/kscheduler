// package com.rhysmccaig.kscheduler.processor;

// import org.apache.kafka.streams.processor.Processor;
// import org.apache.kafka.streams.processor.ProcessorContext;
// import org.apache.kafka.streams.state.KeyValueStore;
// import org.apache.logging.log4j.LogManager;
// import org.apache.logging.log4j.Logger;

// public class ScheduleProcessor implements Processor<byte[], byte[]> {
//   static final Logger logger = LogManager.getLogger(ScheduleProcessor.class); 

//   private ProcessorContext context;
//   private KeyValueStore<String, Long> kvStore;

//   @Override
//   @SuppressWarnings("unchecked")
//   public void init(ProcessorContext context) {
//     this.context = context;
//     kvStore = (KeyValueStore) context.getStateStore("Scheduler")
//   }

// }