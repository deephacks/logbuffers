package org.deephacks.logbuffers;


import java.util.Iterator;

public abstract class AbstractIterable<T> implements Iterable<T> {
  protected abstract T computeNext();

  @Override
  public Iterator<T> iterator() {

    return new Iterator<T>() {
      T next;

      @Override
      public boolean hasNext() {
        next = computeNext();
        return next != null;
      }

      @Override
      public T next() {
        return next;
      }
    };
  }
}
