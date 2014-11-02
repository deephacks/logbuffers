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
import java.util.*;

class TailerHolder {
  /**
   * startIndexForTime -> tailer
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

  /**
   * @deprecated
   */
  public Optional<LogRaw> binarySearchAfterTime(long time) throws IOException {
    Tailer tailer = getClosestTailerBeforeTime(time);
    long lastWrittenIndex = tailer.getLastWrittenIndex();
    long low = tailer.startIndex;
    long high = lastWrittenIndex;
    long index = -1;
    synchronized (this) {
      while (low < high) {
        long mid = (low + high) >>> 1;
        Optional<Long> optional = LogRaw.peekTimestamp(tailer, mid);
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
        index = low - 1 > lastWrittenIndex ? low : low - 1;
      }
    }
    return LogRaw.read(this, index);
  }

  /**
   * @deprecated
   */
  Optional<ExcerptTailer> getExcerptTailerForIndex(long index) {
    long firstIndex = ranges.startIndexForIndex(index);
    Tailer tailer = tailers.get(firstIndex);
    if (tailer != null) {
      long lastWrittenIndex = tailer.getLastWrittenIndex();
      if (index <= lastWrittenIndex) {
        return Optional.of(tailer.tailer);
      }
      long nextStartIndex = ranges.nextStartIndexForIndex(firstIndex);
      String startTimeFormat = ranges.startTimeFormatForIndex(nextStartIndex);
      try {
        IndexedChronicle chronicle = new IndexedChronicle(basePath.toString() + "/" + startTimeFormat + "/" + startTimeFormat);
        ExcerptTailer excerptTailer = chronicle.createTailer();
        Tailer chronicleTailerPair = new Tailer(nextStartIndex, ranges, chronicle, excerptTailer);
        tailers.put(nextStartIndex, chronicleTailerPair);
        return Optional.of(excerptTailer);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return Optional.absent();
  }

  /**
   * Get the latest possible index that have been written up until now.
   */
  public long getLatestStopIndex() {
    return ranges.stopIndexForTime(System.currentTimeMillis());
  }

  /**
   * The first index that is known.
   */
  public long getFirstIndex() {
    return firstIndex;
  }

  /**
   * Converts a global index to a local index understood by a specific ExcerptTailer.
   */
  public long convertToLocalIndex(long index) {
    return index - ranges.startIndexForIndex(index);
  }

  /**
   * Return all known data between two, both inclusive, instants. Unpredictable behaviour
   * will occur if there are holes in the data, i.e. missing intervals.
   */
  public LinkedList<Tailer> getTailersBetweenTime(long fromTime, long toTime) {
    LinkedList<Tailer> tailers = new LinkedList<>();
    long currentIndex = ranges.startIndexForTime(fromTime);
    long stopIndex = ranges.nextStartIndexForIndex(ranges.startIndexForTime(toTime));
    while (currentIndex < stopIndex) {
      Tailer tailer = this.tailers.get(currentIndex);
      if (tailer != null) {
        tailers.add(tailer);
      } else {
        try {
          Optional<Tailer> optional = initalizeTailer(currentIndex);
          if (!optional.isPresent()) {
            return tailers;
          }
          this.tailers.put(currentIndex, optional.get());
          tailers.add(optional.get());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      currentIndex = ranges.nextStartIndexForIndex(currentIndex);
    }
    return tailers;
  }

  /**
   * Return all known data between two, both inclusive, indexes. Unpredictable behaviour
   * will occur if there are holes in the data, i.e. missing intervals.
   */
  public List<Tailer> getTailersBetweenIndex(long fromIndex, long toIndex) {
    List<Tailer> tailers = new ArrayList<>();
    long currentIndex = fromIndex;
    while (currentIndex <= toIndex) {
      Tailer tailer = this.tailers.get(currentIndex);
      if (tailer != null) {
        tailers.add(tailer);
      } else {
        try {
          Optional<Tailer> optional = initalizeTailer(currentIndex);
          if (!optional.isPresent()) {
            return tailers;
          }
          this.tailers.put(currentIndex, optional.get());
          tailers.add(optional.get());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      currentIndex = ranges.nextStartIndexForIndex(currentIndex);
    }
    return tailers;
  }

  public void close() throws IOException {
    for (Tailer chronicleTailerPair : tailers.values()) {
      chronicleTailerPair.tailer.close();
      chronicleTailerPair.chronicle.close();
    }
  }

  private long initalize() {
    long firstIndex = Long.MAX_VALUE;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(basePath)) {
      for (Path p : stream) {
        long startIndex = ranges.startIndex(p.getFileName().toString());
        if (startIndex < firstIndex) {
          firstIndex = startIndex;
        }
        Optional<Tailer> optional = initalizeTailer(startIndex);
        if (!optional.isPresent()) {
          continue;
        }
        tailers.put(startIndex, optional.get());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (firstIndex == Long.MAX_VALUE) {
      throw new IllegalArgumentException("No data found in " + basePath);
    }
    return firstIndex;
  }

  private Optional<Tailer> initalizeTailer(long startIndex) throws IOException {
    String intervalDir = ranges.startTimeFormatForIndex(startIndex);
    File chronicleDir = new File(basePath.toString(), intervalDir);
    if (!chronicleDir.exists()) {
      return Optional.absent();
    }
    String chroniclePath = chronicleDir.getAbsolutePath() + "/" + intervalDir;
    IndexedChronicle chronicle = new IndexedChronicle(chroniclePath);
    ExcerptTailer excerptTailer = chronicle.createTailer();
    return Optional.of(new Tailer(startIndex, ranges, chronicle, excerptTailer));
  }


  private Tailer getClosestTailerBeforeTime(long time) {
    long startIndex = ranges.startIndexForTime(time);
    Tailer tailer = tailers.get(startIndex);
    if (tailer != null) {
      return tailer;
    }
    if (startIndex < tailers.firstKey()) {
      return tailers.firstEntry().getValue();
    } else if (startIndex > tailers.lastKey()) {
      return tailers.lastEntry().getValue();
    }
    while ((startIndex = ranges.nextStartIndexForIndex(startIndex)) < tailers.lastKey()) {
      tailer = tailers.get(startIndex);
      if (tailer != null) {
        return tailer;
      }
    }
    throw new IllegalArgumentException("No data exist for time " + time);
  }


  static class Tailer {
    final IndexedChronicle chronicle;
    final ExcerptTailer tailer;
    final DateRanges ranges;
    final long startIndex;
    long stopIndex;
    final long finalizedTime;
    long lastWrittenIndex = -1;

    public Tailer(Long startIndex, DateRanges ranges, IndexedChronicle chronicle, ExcerptTailer tailer) {
      this.ranges = ranges;
      this.tailer = tailer;
      this.chronicle = chronicle;
      this.startIndex = startIndex;
      this.stopIndex = ranges.nextStartIndexForIndex(startIndex) - 1;
      this.finalizedTime = ranges.stopTimeForIndex(startIndex);
    }

    public long getLastWrittenIndex() {
      if (finalizedTime < System.currentTimeMillis() && lastWrittenIndex != -1) {
        return lastWrittenIndex;
      }
      long theLastIndex = chronicle.findTheLastIndex();
      lastWrittenIndex = startIndex + theLastIndex;
      return lastWrittenIndex;
    }

    public long getHolderIndex(long index) {
      return index - ranges.startIndexForIndex(index);
          }

    public Optional<LogRaw> binarySearchAfterTime(long time) {
      long lastWrittenIndex = getLastWrittenIndex();
      long low = startIndex;
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
          index = low > lastWrittenIndex ? lastWrittenIndex : low;
        }
      }
      return LogRaw.read(this, index);
    }
  }
}
