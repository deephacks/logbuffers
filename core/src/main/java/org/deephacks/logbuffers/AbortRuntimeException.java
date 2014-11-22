package org.deephacks.logbuffers;

public class AbortRuntimeException extends RuntimeException {
  public AbortRuntimeException(String message) {
    super(message);
  }
}
