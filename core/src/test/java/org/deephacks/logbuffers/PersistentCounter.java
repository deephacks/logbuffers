package org.deephacks.logbuffers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class PersistentCounter {
  private Path basePath;
  public PersistentCounter(String basePath) throws FileNotFoundException {
    File file = new File(basePath);
    file.mkdirs();
    this.basePath = Paths.get(basePath);
  }


  public PersistentCounter() throws FileNotFoundException {
    this("/tmp/logbuffer");
  }

  public long getAndIncrement() {
    try {
      byte[] bytes = Files.readAllBytes(basePath);
      long counter = -1;
      if (bytes.length == 8) {
        counter = LogUtil.getLong(bytes);
      }
      Files.write(basePath, LogUtil.toBytes(counter++), StandardOpenOption.CREATE);
      return counter;
    } catch (IOException e) {
      throw new AbortRuntimeException(e.getMessage() + ": " + e.getMessage());
    }
  }
}
