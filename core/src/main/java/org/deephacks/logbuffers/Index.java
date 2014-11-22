/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.deephacks.logbuffers;

import javafx.util.Pair;
import net.openhft.lang.io.VanillaMappedBytes;
import net.openhft.lang.io.VanillaMappedFile;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

/**
 * Keeps track of the timestamp for a tail.
 */
interface Index {

  void writeLastSeen(long time, long index);

  long[] getLastSeen();

  /**
   * Store that latest timestamp and index in binary format. This is many times
   * faster and should be considered for indexes that updates at a fast pace.
   */
  public static Index binaryIndex(String path) throws IOException {
    return new BinaryIndex(path);
  }

  /**
   * Store that latest timestamp and index in human readable text format.
   * This can be somewhat convenient is the timestamp or index must be
   * adjusted manually.
   */
  public static Index textIndex(String path) throws IOException {
    return new TextIndex(path);
  }

  static class TextIndex implements Index {

    static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private Path filePath;

    TextIndex(String filePath) throws IOException {
      this.filePath = Paths.get(filePath);
      if (!this.filePath.toFile().exists()) {
        writeLastSeen(0, -1);
      }
    }

    public synchronized void writeLastSeen(long time, long index) {
      try {
        Date date = new Date(time);
        String timeFormat = format.format(date);
        // we must have both fromTime and index in order to not process to few logs
        String line = timeFormat + " " + index;
        Files.write(filePath, line.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public synchronized long[] getLastSeen() {
      byte[] bytes;
      try {
        bytes = Files.readAllBytes(filePath);
      } catch (IOException e) {
        throw new AbortRuntimeException(e.getMessage() + ": " + e.getMessage());
      }
      String line = new String(bytes, StandardCharsets.UTF_8);
      String timeFormat;
      long index = -1;
      if (line.contains(" ")) {
        String[] split = line.split(" ");
        timeFormat = split[0];
        index = Long.parseLong(split[1]);
      } else {
        timeFormat = line.trim();
      }
      try {
        Date date = format.parse(timeFormat.trim());
        return new long[] { date.getTime(), index };
      } catch (ParseException e) {
        throw new IllegalArgumentException("Could not parse " +  timeFormat + "  from file " + filePath);
      }
    }
  }


  static class BinaryIndex implements Index {
    private final VanillaMappedFile file;
    private VanillaMappedBytes bytes;

    BinaryIndex(String filePath) throws IOException {
      File f = new File(filePath);
      if (!f.exists()) {
        this.file = VanillaMappedFile.readWrite(f);
        this.bytes = file.bytes(0, 16);
        writeLastSeen(0, -1);
      } else {
        this.file = VanillaMappedFile.readWrite(f);
        this.bytes = file.bytes(0, 16);
      }
    }

    @Override
    public synchronized void writeLastSeen(long time, long index) {
      bytes.position(0);
      bytes.writeLong(time);
      bytes.writeLong(index);
    }

    @Override
    public synchronized long[] getLastSeen() {
      bytes.position(0);
      long timestamp = bytes.readLong();
      long index = bytes.readLong();
      return new long[] { timestamp, index };
    }
  }
}
