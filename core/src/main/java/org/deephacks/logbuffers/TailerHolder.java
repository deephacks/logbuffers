package org.deephacks.logbuffers;

import com.google.common.base.Optional;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.TreeMap;

class TailerHolder {
  /**
   * startIndex -> tailer
   */
  private final TreeMap<Long, Tailer> tailers = new TreeMap<>();
  private final DateRanges ranges;
  private final Path basePath;
  private IndexedChronicle chronicle;
  private final long firstIndex;

  TailerHolder(String path, DateRanges ranges) {
    this.ranges = ranges;
    this.basePath = Paths.get(path);
    this.firstIndex = initalize();
  }

  private long initalize() {
    long firstIndex = Long.MAX_VALUE;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(basePath)) {
      for (Path p : stream) {
        long startIndex = ranges.startIndex(p.getFileName().toString());
        if (startIndex < firstIndex) {
          firstIndex = startIndex;
        }
        IndexedChronicle chronicle = new IndexedChronicle(basePath.toString() + "/" + p.getFileName() + "/" + p.getFileName());
        ExcerptTailer tailer = chronicle.createTailer();
        Tailer chronicleTailerPair = new Tailer(startIndex, ranges, chronicle, tailer);
        tailers.put(startIndex, chronicleTailerPair);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (firstIndex == Long.MAX_VALUE) {
      throw new IllegalArgumentException("No data found in " + basePath);
    }
    return firstIndex;
  }

  public Tailer getClosestTailerForTime(long time) {
    long startIndex = ranges.startIndex(time);
    Tailer tailer = tailers.get(startIndex);
    if (tailer != null) {
      return tailer;
    }
    if (startIndex < tailers.firstKey()) {
      return tailers.firstEntry().getValue();
    } else if (startIndex > tailers.lastKey()) {
      return tailers.lastEntry().getValue();
    }
    while ((startIndex = ranges.nextStartIndex(startIndex)) < tailers.lastKey()) {
      tailer = tailers.get(startIndex);
      if (tailer != null) {
        return tailer;
      }
    }
    throw new IllegalArgumentException("No data exist for time " + time);
  }


  /**
   * Search the closest index to start time using binary search.
   */
  public Optional<LogRaw> binarySearchAfterTime(long time) throws IOException {
    Tailer tailer = getClosestTailerForTime(time);
    long lastWrittenIndex = tailer.getLastWrittenIndex();
    long low = tailer.startIndex;
    long high = lastWrittenIndex;
    long index = -1;
    synchronized (this) {
      while (low < high) {
        long mid = (low + high) >>> 1;
        Optional<Long> optional = LogRaw.peekTimestamp(this, mid);
        if (!optional.isPresent()) {
          high = mid - 1;
        } else {
          long timestamp = optional.get();
          if (timestamp < time) {
            low = mid + 1;
          } else if (timestamp > time) {
            high = mid - 1;
          } else {
            index = mid;
          }
        }
      }
      if (index == -1) {
        index = low >= lastWrittenIndex ? lastWrittenIndex - 1 : low;
      }
    }
    return LogRaw.read(this, index);
  }

  public Optional<LogRaw> binarySearchBeforeTime(long time) throws IOException {
    Tailer tailer = getClosestTailerForTime(time);
    long lastWrittenIndex = tailer.getLastWrittenIndex();
    long low = tailer.startIndex;
    long high = lastWrittenIndex;
    long index = -1;
    synchronized (this) {
      while (low < high) {
        long mid = (low + high) >>> 1;
        Optional<Long> optional = LogRaw.peekTimestamp(this, mid);
        if (!optional.isPresent()) {
          high = mid - 1;
        } else {
          long timestamp = optional.get();
          if (timestamp < time) {
            low = mid + 1;
          } else if (timestamp > time) {
            high = mid - 1;
          } else {
            index = mid;
            break;
          }
        }
      }
      if (index == -1) {
        index = low + 1 > lastWrittenIndex ? low : low + 1;
      }
    }
    return LogRaw.read(this, index);
  }


  Optional<ExcerptTailer> getTailerForIndex(long index) {
    long firstIndex = ranges.getFirstIndexOfIntervalIndex(index);
    Tailer chronicleTailerPair = tailers.get(firstIndex);
    if (chronicleTailerPair != null) {
      return Optional.of(chronicleTailerPair.tailer);
    }
    return Optional.absent();
    /*
    try {
      String intervalDir = ranges.getStartTimeFormat(index);
      File chronicleDir = new File(basePath.toString(), intervalDir);
      if (!chronicleDir.exists()) {
        throw new IllegalArgumentException("No data for index " + index);
      }

      if (chronicle == null) {
        this.chronicle = new IndexedChronicle(chronicleDir.getAbsolutePath() + "/" + intervalDir + "/" + intervalDir);
      }
      ExcerptTailer tailer = chronicle.createTailer();
      chronicleTailerPair = new ChronicleTailerPair(chronicle, tailer);
      tailers.put(index, chronicleTailerPair);
      return tailer;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    */
  }


  public void close() throws IOException {
    for (Tailer chronicleTailerPair : tailers.values()) {
      chronicleTailerPair.tailer.close();
      chronicleTailerPair.chronicle.close();
    }
  }

  public long getStopIndex(long time) {
    return ranges.stopIndex(time);
  }

  public long getStartIndex(long time) {
    return ranges.startIndex(time);
  }

  public long getCurrentStopIndex() {
    return getStopIndex(System.currentTimeMillis());
  }

  public long getFirstWrittenIndex() {
    return firstIndex;
  }

  public long getHolderIndex(long index) {
    return index - ranges.getFirstIndexOfIntervalIndex(index);
  }

  private static class Tailer {
    private final IndexedChronicle chronicle;
    private final ExcerptTailer tailer;
    private final DateRanges ranges;
    private final long startIndex;
    private final long stopIndex;

    public Tailer(Long startIndex, DateRanges ranges, IndexedChronicle chronicle, ExcerptTailer tailer) {
      this.ranges = ranges;
      this.tailer = tailer;
      this.chronicle = chronicle;
      this.startIndex = startIndex;
      this.stopIndex = ranges.nextStartIndex(startIndex) - 1;
    }

    public long getLastWrittenIndex() {
      return startIndex + chronicle.findTheLastIndex();
    }
  }
}
