package com.rhysmccaig.kscheduler.util;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;

public class SerializationUtils {

  private SerializationUtils() {}

  /**
   * Returns a byte array representing a long value.
   * the most significant bit is flipped so that the resulting
   * byte array will sort lexicographically
   * @param val the long value to convert to bytes
   * @return
   */
  public static byte[] toOrderedBytes(long val) {
    return new byte[] {
      (byte) ((val >> 56) ^ 0x80),
      (byte) (val >> 48),
      (byte) (val >> 40),
      (byte) (val >> 32),
      (byte) (val >> 24),
      (byte) (val >> 16),
      (byte) (val >> 8),
      (byte) val
    };
  }

  /**
   * Returns a byte array representing an int value.
   * the most significant bit is flipped so that the resulting
   * byte array will sort lexicographically
   * @param val the long value to convert to bytes
   * @return
   */
  public static byte[] toOrderedBytes(int val) {
    return new byte[] {
      (byte) ((val >> 24) ^ 0x80),
      (byte) (val >> 16),
      (byte) (val >> 8),
      (byte) val
    };
  }

    /**
   * Returns a byte array representing an UUID value.
   * the most significant bits first so that
   * byte array will sort lexicographically
   * @param val the long value to convert to bytes
   * @return
   */
  public static byte[] toOrderedBytes(UUID val) {
    var bytes = new byte[16];
    ByteBuffer.allocate(16)
        .putLong(val.getMostSignificantBits())
        .putLong(val.getLeastSignificantBits())
        .flip()
        .get(bytes);
    return bytes;
  }

  /**
   * Converts byte array that has been constructed to order lexicographically into a long.
   * @param bytes the bytes to convert into a long
   * @return
   */
  public static long longFromOrderedBytes(byte[] bytes) {
    long val = (bytes[0] ^ 0x80) & 0xff;
    for (int i = 1; i < 8; i++) {
      val = (val << 8) + (bytes[i] & 0xff);
    }
    return val;
  }

  /**
   * Converts byte array that has been constructed to order lexicographically into an int.
   * @param bytes the bytes to convert into a int
   * @return
   */
  public static int intFromOrderedBytes(byte[] bytes) {
    int val = (bytes[0] ^ 0x80) & 0xff;
    for (int i = 1; i < 4; i++) {
      val = (val << 8) + (bytes[i] & 0xff);
    }
    return val;
  }

  // Put a long into a ByteBuffer encoded to be ordered lexicographically 
  public static ByteBuffer putOrderedBytes(ByteBuffer buffer, long l) {
    return buffer.put(toOrderedBytes(l));
  }

  // Put an int ByteBuffer a ByteBuffer encoded to be ordered lexicographically 
  public static ByteBuffer putOrderedBytes(ByteBuffer buffer, int i) {
    return buffer.put(toOrderedBytes(i));
  }

  // Put an Instant into a ByteBuffer encoded to be ordered lexicographically 
  public static ByteBuffer putOrderedBytes(ByteBuffer buffer, Instant instant) {
    return buffer.put(toOrderedBytes(instant.getEpochSecond()))
                 .put(toOrderedBytes(instant.getNano()));
  }

  // Put a UUID into a ByteBuffer encoded to be ordered lexicographically 
  public static ByteBuffer putOrderedBytes(ByteBuffer buffer, UUID uuid) {
    return buffer.putLong(uuid.getMostSignificantBits())
                 .putLong(uuid.getLeastSignificantBits());
  }




  /**
   * Extracts an instant from the bytebuffer that has been encoded to be ordered lexicographically .
   * The bytebuffer's new position will be incremented by 12 (Long.BYTES + Integer.BYTES)
   * @param buffer the bytebuffer to extract the Instant from
   * @return
   */
  public static Instant getInstant(ByteBuffer buffer) {
    byte[] secondsBytes = new byte[Long.BYTES];
    byte[] nanosBytes = new byte[Integer.BYTES];
    buffer.get(secondsBytes)
          .get(nanosBytes);
    var seconds = SerializationUtils.longFromOrderedBytes(secondsBytes);
    var nanos = SerializationUtils.intFromOrderedBytes(nanosBytes);
    // This will throw a DatetimeException if the long value representing seconds 
    // is out of the range supported by an Instant
    return Instant.ofEpochSecond(seconds, nanos);
  }

  /**
   * Extracts a UUID from the bytebuffer that has been encoded to be ordered lexicographically .
   * The bytebuffer's new position will be incremented by 16 (Long.BYTES + Long.BYTES)
   * @param buffer the bytebuffer to extract the Instant from
   * @return
   */
  public static UUID getUuid(ByteBuffer buffer) {
    var mostSigBits = buffer.getLong();
    var leastSigBits = buffer.getLong();
    return new UUID(mostSigBits, leastSigBits);
  }

}