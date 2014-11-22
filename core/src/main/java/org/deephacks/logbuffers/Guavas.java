package org.deephacks.logbuffers;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class Guavas {
  public static <T> T checkNotNull(T reference, String message) {
    if (reference == null) {
      throw new NullPointerException(message);
    }
    return reference;
  }

  public static <T> T checkNotNull(T reference) {
    if (reference == null) {
      throw new NullPointerException();
    }
    return reference;
  }

  public static void checkArgument(boolean expression, String msg) {
    if (!expression) {
      throw new IllegalArgumentException(msg);
    }
  }
  public static <E> ArrayList<E> newArrayList(Iterable<? extends E> elements) {
    checkNotNull(elements);
    ArrayList<E> list = new ArrayList<>();
    elements.forEach(list::add);
    return list;
  }
  public static <E> ArrayList<E> newArrayList(E... elements) {
    checkNotNull(elements);
    ArrayList<E> list = new ArrayList<>();
    Collections.addAll(list, elements);
    return list;
  }

  public static <T> Stream<T> toStream(Iterable<T> it, boolean parallel) {
    Spliterator<T> spliterator = Spliterators.spliterator(it.iterator(), 10, Spliterator.CONCURRENT);
    return StreamSupport.stream(spliterator, parallel);
  }

  public static boolean isNullOrEmpty(String string) {
    return string == null || string.length() == 0;
  }

}
