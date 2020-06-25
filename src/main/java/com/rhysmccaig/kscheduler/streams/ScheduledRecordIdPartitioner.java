package com.rhysmccaig.kscheduler.streams;


import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import java.nio.ByteBuffer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class ScheduledRecordIdPartitioner implements StreamPartitioner<ScheduledRecordMetadata, ScheduledRecord> {

  private static int ID_SIZE = 16;
  private static ThreadLocal<ByteBuffer> TL_BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocate(ID_SIZE));

  /**
   * Partitions by the scheduled record ID.
   */
  @Override
  public Integer partition(
      final String topic, final ScheduledRecordMetadata key, final ScheduledRecord value, final int numPartitions) {
    var buffer = TL_BUFFER.get().clear();
    var idBytes = new byte[ID_SIZE];
    buffer.putLong(key.id().getMostSignificantBits())
          .putLong(key.id().getLeastSignificantBits())
          .flip()
          .get(idBytes); 
    return Utils.toPositive(Utils.murmur2(idBytes)) % numPartitions;
  }

}