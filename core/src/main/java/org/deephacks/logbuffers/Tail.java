package org.deephacks.logbuffers;

import java.util.List;

public interface Tail<T> {

    public void process(List<T> messages);

    public String getName();
}
