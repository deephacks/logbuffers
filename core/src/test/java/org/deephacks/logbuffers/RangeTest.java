package org.deephacks.logbuffers;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RangeTest {

  @Test
  public void rangedShouldBeConnected() {
    Range r1 = Range.closed(1, 3);
    Range r2 = Range.closed(3, 6);
    assertTrue(r1.isConnected(r2));
    assertTrue(r1.contains(1));
    assertTrue(r1.contains(3));

    assertTrue(r2.isConnected(r1));
    assertTrue(r2.contains(3));
    assertTrue(r2.contains(6));
  }

  @Test
  public void rangesShouldNotBeConnected() {
    Range r1 = Range.closed(1, 3);
    Range r2 = Range.closed(4, 6);
    assertFalse(r1.isConnected(r2));
    assertFalse(r1.contains(0));
    assertFalse(r1.contains(4));

    assertFalse(r2.isConnected(r1));
    assertFalse(r1.contains(4));
    assertFalse(r1.contains(7));
  }
}
