package org.deephacks.logbuffers.protobuf;


import org.deephacks.logbuffers.*;
import org.deephacks.logbuffers.protobuf.ProtoLog.PageView;
import org.deephacks.logbuffers.protobuf.ProtoLog.Visit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class ProtobufLogBufferTest {

  PageViewTail pageViewTail;
  VisitTail visitTail;

  LogBuffer logBuffer;

  PageView p1 = PageView.newBuilder().setUrl("www.google.com").setValue(1).build();
  PageView p2 = PageView.newBuilder().setUrl("www.cloudera.com").setValue(2).build();
  Visit v1 = Visit.newBuilder().setUrl("www.apache.org").setValue(1).build();
  Visit v2 = Visit.newBuilder().setUrl("www.google.com").setValue(2).build();

  String path;
  @Before
  public void before() throws IOException {
    if (logBuffer != null) {
      logBuffer.close();
    }
    this.path = LogUtil.cleanupTmpDir();
    logBuffer = LogBuffer.newBuilder()
            .secondly()
            .basePath(path)
            .addSerializer(new ProtobufSerializer())
            .build();
    pageViewTail = new PageViewTail();
    visitTail = new VisitTail();
  }

  @After
  public void after() throws IOException {
    logBuffer.close();
  }

  @Test
  public void test_write_read_different_types() throws IOException {
    // one type log
    LogRaw first = logBuffer.write(p1);
    logBuffer.write(p2);
    List<PageView> pageViews = logBuffer.select(PageView.class, first.getIndex()).get();

    assertThat(pageViews.get(0).getUrl(), is(p1.getUrl()));
    assertThat(pageViews.get(0).getValue(), is(p1.getValue()));
    assertThat(pageViews.get(1).getUrl(), is(p2.getUrl()));
    assertThat(pageViews.get(1).getValue(), is(p2.getValue()));

    // another type log
    logBuffer.write(v1);
    logBuffer.write(v2);
    List<Visit> visits = logBuffer.select(Visit.class, first.getIndex()).get();

    assertThat(visits.get(0).getUrl(), is(v1.getUrl()));
    assertThat(visits.get(0).getValue(), is(v1.getValue()));
    assertThat(visits.get(1).getUrl(), is(v2.getUrl()));
    assertThat(visits.get(1).getValue(), is(v2.getValue()));
  }

  @Test
  public void test_write_tail_different_types() throws IOException {
    TailSchedule visitSchedule = TailSchedule.builder(visitTail).build();
    TailSchedule pageViewSchedule = TailSchedule.builder(pageViewTail).build();

    logBuffer.write(p1);
    logBuffer.write(v1);
    logBuffer.write(p2);
    logBuffer.write(v2);

    logBuffer.forward(visitSchedule);
    assertThat(visitTail.logs.size(), is(2));
    assertThat(pageViewTail.logs.size(), is(0));
    assertThat(visitTail.logs.get(0).getUrl(), is(v1.getUrl()));
    assertThat(visitTail.logs.get(1).getUrl(), is(v2.getUrl()));

    logBuffer.forward(pageViewSchedule);
    assertThat(visitTail.logs.size(), is(2));
    assertThat(pageViewTail.logs.size(), is(2));
    assertThat(pageViewTail.logs.get(0).getUrl(), is(p1.getUrl()));
    assertThat(pageViewTail.logs.get(1).getUrl(), is(p2.getUrl()));

  }

  public static class PageViewTail implements Tail<PageView> {
    List<PageView> logs = new ArrayList<>();

    @Override
    public void process(Logs<PageView> logs) {
      this.logs.addAll(logs.get());
    }
  }

  public static class VisitTail implements Tail<Visit> {
    List<Visit> logs = new ArrayList<>();

    @Override
    public void process(Logs<Visit> logs) {
      this.logs.addAll(logs.get());
    }
  }

}
