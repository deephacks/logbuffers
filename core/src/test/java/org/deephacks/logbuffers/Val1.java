package org.deephacks.logbuffers;

import org.deephacks.vals.Encodable;
import org.deephacks.vals.Id;
import org.deephacks.vals.Val;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Val
public interface Val1 extends Encodable {
  @Id(0) String getString();
  @Id(1) long getPLong();
  @Id(2) List<String> getStringList();
  @Id(3) byte[] getByteArray();
  @Id(4) Map<TimeUnit, Integer> getEnumIntegerMap();
  @Id(5) Map<String, Val2> getStringVal2Map();
}
