package com.rhysmccaig.kscheduler.util;

public class SerializationUtils {
  
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

  public static byte[] toOrderedBytes(int val) {
    byte[] bytes = new byte[] {
      (byte) ((val >> 24) ^ 0x80),
      (byte) (val >> 16),
      (byte) (val >> 8),
      (byte) val
    };
    return bytes;
  }

  public static long longFromOrderedBytes(byte[] bytes) {
    long val = (bytes[0] ^ 0x80) & 0xff;
    for (int i = 1; i < 8; i++) {
      val = (val << 8) + (bytes[i] & 0xff);
    }
    return val;
  }

  public static int intFromOrderedBytes(byte[] bytes) {
    int val = (bytes[0] ^ 0x80) & 0xff;
    for (int i = 1; i < 4; i++) {
      val = (val << 8) + (bytes[i] & 0xff);
    }
    return val;
  }

}