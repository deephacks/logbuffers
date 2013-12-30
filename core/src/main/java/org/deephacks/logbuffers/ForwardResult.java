package org.deephacks.logbuffers;


class ForwardResult {
  private boolean finished = true;

  ForwardResult() {

  }

  ForwardResult(boolean finished) {
    this.finished = finished;
  }

  boolean isFinished() {
    return finished;
  }
}
