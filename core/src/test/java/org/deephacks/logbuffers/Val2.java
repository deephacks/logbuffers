package org.deephacks.logbuffers;

import org.deephacks.vals.Encodable;
import org.deephacks.vals.Id;
import org.deephacks.vals.Val;

import java.util.concurrent.TimeUnit;

@Val
public interface Val2 extends Encodable {
  @Id(0) String getString();
  @Id(1) TimeUnit getTimeUnit();
}
