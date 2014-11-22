package org.deephacks.logbuffers.cli;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Used for manual consistency checks while producing 100.000 logs per seconds
 * on very short (seconds) rolling files. Have been tried for one billion
 * logs without loss.
 *
 */
public class TestSeq {

  public static void main(String[] args) throws IOException {
    checkLogConsistency();
  }

  /**
   * Use the test command 'seq' to write logs with a sequential number in
   * their contents. At the same fromTime start a 'tail' that pipe output to /tmp/log.
   * <p/>
   * 1) lb seq -i seconds
   * 2) lb tail > /tmp/log
   * <p/>
   * This method checks that logs have fully sequential number series.
   */
  public static void checkLogConsistency() throws IOException {
    long previousSeq = -1;
    long numLines = 0;
    try (BufferedReader br = new BufferedReader(new FileReader("/tmp/log"))) {
      String line;
      while ((line = br.readLine()) != null) {
        numLines++;
        long currentSeq = Long.valueOf(line.split(" ")[2]);
        if (previousSeq == -1) {
          previousSeq = currentSeq;
        } else {
          if (previousSeq + 1 != currentSeq) {
            throw new IllegalStateException(previousSeq + " " + currentSeq);
          }
          previousSeq = currentSeq;
        }
        if (numLines % 1000000 == 0) {
          System.out.println(numLines);
        }
      }
    } catch (IOException e) {
      throw e;
    }
    System.out.println(numLines);
  }
}
