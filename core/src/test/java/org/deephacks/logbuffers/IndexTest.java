package org.deephacks.logbuffers;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class IndexTest {
  String dir;

  @Before
  public void before() {
    dir = LogUtil.cleanupTmpDir();
    new File(dir).mkdirs();
    dir += "test";
  }

  @Test
  public void write_read_close_read() throws IOException {
    Index index = Index.binaryIndex(dir);
    index.writeLastSeen(0, 1);
    long[] lastSeen = index.getLastSeen();
    assertThat(lastSeen[0], is(0L));
    assertThat(lastSeen[1], is(1L));
  }
}
