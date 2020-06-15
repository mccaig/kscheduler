package com.rhysmccaig.kscheduler.util;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;

public class SerializationUtils {

  private static final ThreadLocal<ByteBuffer> TL_BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocate(0));


  // returns a byte array representing a long value
  // the most significant bit is flipped so that the resulting
  // byte array will sort lexicographically
  public static byte[] toOrderedBytes(long val) {
    byte[] bytes = new byte[] {
      (byte) ((val >> 56) ^ 0x80),
      (byte) (val >> 48),
      (byte) (val >> 40),
      (byte) (val >> 32),
      (byte) (val >> 24),
      (byte) (val >> 16),
      (byte) (val >> 8),
      (byte) val
    };
    return bytes;
  }

  // returns a byte array representing a int value
  // the most significant bit is flipped so that the resulting
  // byte array can be compared lexicographically
  public static byte[] toOrderedBytes(int val) {
    byte[] bytes = new byte[] {
      (byte) ((val >> 24) ^ 0x80),
      (byte) (val >> 16),
      (byte) (val >> 8),
      (byte) val
    };
    return bytes;
  }

  // returns a long from a byte array that has been constructed
  // so that it can be compared lexicographically
  public static long longFromOrderedBytes(byte[] bytes) {
    long val = (bytes[0] ^ 0x80) & 0xff;
    for (int i = 1; i < 8; i++) {
      val = (val << 8) + (bytes[i] & 0xff);
    }
    return val;
  }

  // returns a int from a byte array that has been constructed
  // so that it can be compared lexicographically
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

  // Extracts an instant from the bytebuffer that has been encoded to be ordered lexicographically 
  // The bytebuffer's new position will be incremented by 12 (Long.BYTES + Integer.BYTES)
  public static Instant getInstant(ByteBuffer buffer) {
    byte[] secondsBytes = new byte[Long.BYTES];
    byte[] nanosBytes = new byte[Integer.BYTES];
    buffer.get(secondsBytes).get(nanosBytes);
    var seconds = SerializationUtils.longFromOrderedBytes(secondsBytes);
    buffer.get(nanosBytes);
    var nanos = SerializationUtils.intFromOrderedBytes(nanosBytes);
    // This will throw a DatetimeException if the long value representing seconds 
    // is out of the range supported by an Instant
    return Instant.ofEpochSecond(seconds, nanos);
  }

  public static UUID getUUID(ByteBuffer buffer) {
    var mostSigBits = buffer.getLong();
    var leastSigBits = buffer.getLong();
    return new UUID(mostSigBits, leastSigBits);
  }

}