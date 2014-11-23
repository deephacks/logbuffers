package org.deephacks.logbuffers;

import org.deephacks.vals.Encodable;
import org.deephacks.vals.Id;
import org.deephacks.vals.Val;

@Val
public interface PageView extends Encodable {
  @Id(0) String getUrl();
  @Id(1) Long getUserId();
}
