package com.rhysmccaig.kscheduler.streams;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class ToOriginalRecordProcessor implements Processor<byte[], byte[]> {
  
  private ProcessorContext context;

  public ToOriginalRecordProcessor() {}

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    this.context = context;
  }

  public void process(byte[] key, byte[] value) {
  }

  public void close() {}

}