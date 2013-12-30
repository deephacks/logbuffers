package org.deephacks.logbuffers.protobuf;


import org.deephacks.logbuffers.LogBuffer;
import org.deephacks.logbuffers.LogBuffer.Builder;
import org.deephacks.logbuffers.LogUtil;
import org.deephacks.logbuffers.Logs;
import org.deephacks.logbuffers.Tail;
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

  PageView p1 = PageView.newBuilder().setMsg("hello1").setCode(1).build();
  PageView p2 = PageView.newBuilder().setMsg("hello2").setCode(2).build();
  Visit v1 = Visit.newBuilder().setMsg("vist1").setCode(1).build();
  Visit v2 = Visit.newBuilder().setMsg("vist2").setCode(2).build();


  @Before
  public void before() throws IOException {
    logBuffer = new Builder()
            .basePath(LogUtil.tmpDir())
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
    logBuffer.write(p1);
    logBuffer.write(p2);
    List<PageView> pageViews = logBuffer.select(PageView.class, 0).getObjects();

    assertThat(pageViews.get(0).getMsg(), is(p1.getMsg()));
    assertThat(pageViews.get(0).getCode(), is(p1.getCode()));
    assertThat(pageViews.get(1).getMsg(), is(p2.getMsg()));
    assertThat(pageViews.get(1).getCode(), is(p2.getCode()));

    // another type log
    logBuffer.write(v1);
    logBuffer.write(v2);
    List<Visit> visits = logBuffer.select(Visit.class, 0).getObjects();

    assertThat(visits.get(0).getMsg(), is(v1.getMsg()));
    assertThat(visits.get(0).getCode(), is(v1.getCode()));
    assertThat(visits.get(1).getMsg(), is(v2.getMsg()));
    assertThat(visits.get(1).getCode(), is(v2.getCode()));
  }

  @Test
  public void test_write_tail_different_types() throws IOException {

    logBuffer.write(p1);
    logBuffer.write(v1);
    logBuffer.write(p2);
    logBuffer.write(v2);

    logBuffer.forward(visitTail);
    assertThat(visitTail.logs.size(), is(2));
    assertThat(pageViewTail.logs.size(), is(0));
    assertThat(visitTail.logs.get(0).getMsg(), is(v1.getMsg()));
    assertThat(visitTail.logs.get(1).getMsg(), is(v2.getMsg()));

    logBuffer.forward(pageViewTail);
    assertThat(visitTail.logs.size(), is(2));
    assertThat(pageViewTail.logs.size(), is(2));
    assertThat(pageViewTail.logs.get(0).getMsg(), is(p1.getMsg()));
    assertThat(pageViewTail.logs.get(1).getMsg(), is(p2.getMsg()));

  }

  public static class PageViewTail implements Tail<PageView> {
    List<PageView> logs = new ArrayList<>();

    @Override
    public void process(Logs<PageView> logs) {
      this.logs.addAll(logs.getObjects());
    }
  }

  public static class VisitTail implements Tail<Visit> {
    List<Visit> logs = new ArrayList<>();

    @Override
    public void process(Logs<Visit> logs) {
      this.logs.addAll(logs.getObjects());
    }
  }

}