package org.deephacks.logbuffers.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class JvmCrasher {
  public static void crashJvm() {
    Unsafe unsafe = getUnsafe();
    unsafe.setMemory(0, 1, (byte) 0);
  }

  private static Unsafe getUnsafe() {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      return (Unsafe) field.get(null);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }
}
