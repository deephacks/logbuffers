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

    Index index = new Index(dir);

    index.writeIndex(5);
    assertThat(index.getIndex(), is(5L));

    index.close();

    index = new Index(dir);
    assertThat(index.getIndex(), is(5L));
  }
}
