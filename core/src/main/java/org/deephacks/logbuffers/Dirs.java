package org.deephacks.logbuffers;

import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static java.util.Map.Entry;

/**
 * Dirs orchestrate queries and there are quite a few corner cases to be vary about.
 * <p/>
 * 1)  Logs may have same milli timestamp.
 * 2)  Start/stop time/index far beyond existing directory data.
 * 4)  Missing directories (holes in ranges).
 * 5)  Base path contain unrecognized data or mixed data ranges.
 * 6)  Huge amount of directories in base path.
 * 7)  Padded entries in chronicle.
 * 8)  A time range may be huge so queries need binary search for first range.
 * 9)  Cached directories may exceed max number of file descriptors.
 * 10) A user may base path to a single range directory.
 * <p/>
 * Future improvements.
 * <p/>
 * 9) Backward queries.
 */
class Dirs {
  Logger logger = LoggerFactory.getLogger(Dirs.class.getName());
  TreeMap<Long, Dir> dirs = new TreeMap<>();
  RollingRanges ranges;
  private File basePath;
  private ChronicleConfig config;

  // test only
  Dirs(TreeMap<Long, Dir> dirs, RollingRanges ranges) {
    this.ranges = ranges;
    this.dirs = dirs;
    this.basePath = new File("");
  }

  Dirs(File basePath, RollingRanges ranges, ChronicleConfig config) {
    this.config = config;
    this.basePath = basePath;
    this.ranges = ranges;
    initialize();
    if (this.ranges == null) {
      this.ranges = RollingRanges.hourly();
    }
  }

  public AbstractIterable<Dir> execute(final Query query) {
    return new AbstractIterable<Dir>() {
      Range current;
      Range queryRange = query.getRange();

      @Override
      protected Dir computeNext() {
        int initRetry = 0;

        while (true) {
          current = query.nextRange(current, ranges);
          if (!current.isConnected(queryRange) || pastPresent(current)) {
            // outside query or past present
            logger.debug("done {}", query);
            return null;
          }
          Dir dir = jumpNextDir(getIndexRange(current));
          if (dir == null) {
            if (dirs.isEmpty()) {
              // no dirs
              logger.debug("no dirs {}", basePath);
              initialize();
              if (initRetry++ == 1) {
                return null;
              }
              // started off with empty directory
              continue;
            }
            // current range lacks a dir
            continue;
          }
          logger.debug("processing {}", dir);
          current = query.isIndexQuery() ? dir.getIndexRange() : dir.getTimeRange();
          return dir;
        }
      }

      private boolean pastPresent(Range current) {
        current = query.isTimeQuery() ? current : ranges.toTimeRange(current);
        long now = System.currentTimeMillis();
        return current.start() > now;
      }

      private Range getIndexRange(Range range) {
        return query.isIndexQuery() ? range : ranges.toIndexRange(range);
      }
    };
  }

  public Dir getDir(long index) {
    long startIndex = ranges.startIndexForIndex(index);
    Dir dir = dirs.get(startIndex);
    if (dir != null) {
      return dir;
    }
    dir = Dir.tryCreate(basePath, ranges, startIndex, config);

    if (dir != null) {
      dirs.put(startIndex, dir);
      return dir;
    }
    return null;
  }

  private Dir jumpNextDir(Range indexRange) {
    Dir dir = dirs.get(indexRange.start());
    if (dir != null) {
      logger.debug("Cached dir found for index {}", indexRange.start());
      return dir;
    }
    Entry<Long, Dir> first = dirs.firstEntry();
    if (first != null && indexRange.start() < first.getKey().longValue()) {
      return first.getValue();
    }
    dir = Dir.tryCreate(basePath, ranges, indexRange.start(), config);
    if (dir != null) {
      if (dirs.containsKey(indexRange.start())) {
        return dirs.get(indexRange.start());
      }
      dirs.put(indexRange.start(), dir);
      return dir;
    }
    return null;
  }

  void initialize() {
    if (!dirs.isEmpty()) {
      return;
    }
    if (!basePath.exists()) {
      throw new IllegalArgumentException("Basepath does not exist " + basePath);
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(basePath.toPath())) {
      for (Path p : stream) {
        Dir dir = Dir.tryCreate(p.toFile(), ranges, config);
        if (dir != null) {
          dirs.put(dir.getIndexRange().start(), dir);
          this.ranges = dir.ranges;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() throws IOException {
    for (Dir dir : dirs.values()) {
      dir.close();
    }
  }

  public Collection<Dir> listDirs() {
    if (dirs.isEmpty()) {
      initialize();
    }
    return dirs.values();
  }

  public static class Dir {
    private static Logger logger = LoggerFactory.getLogger(Dir.class.getName());
    protected final Range indexRange;
    protected final Range timeRange;
    private final String basePath;
    private final long indexOffset;
    private final RollingRanges ranges;
    private ChronicleConfig config;
    private IndexedChronicle chronicle;
    private ExcerptTailer tailer;

    Dir(File basePath, RollingRanges ranges, ChronicleConfig config) {
      this.config = config;
      this.ranges = ranges;
      this.timeRange = ranges.timeRange(basePath.getName());
      this.indexRange = ranges.toIndexRange(timeRange);
      this.basePath = basePath.toString();
      this.indexOffset = ranges.indexOffset(timeRange.start());
    }

    public static Dir tryCreate(File basePath, RollingRanges ranges, ChronicleConfig config) {
      Optional<File> dir = getDir(basePath);
      if (!dir.isPresent()) {
        logger.debug("No dir found for base path {}", basePath);
        return null;
      }
      if (ranges == null) {
        ranges = RollingRanges.tryCreate(dir.get().getName());
      }
      return new Dir(dir.get(), ranges, config);
    }

    public static Dir tryCreate(File basePath, RollingRanges ranges, long startIndex, ChronicleConfig config) {
      String intervalDir = ranges.startTimeFormatForIndex(startIndex);
      File dir = new File(basePath, intervalDir);
      if (!dir.exists()) {
        logger.debug("No dir found {}", dir);
        return null;
      }
      return new Dir(new File(dir, intervalDir), ranges, config);
    }


    private static Optional<File> getDir(File file) {
      String name = file.getName();
      if (!isYear(name)) {
        return Optional.empty();
      }
      if (file.isFile() && (name.endsWith("data") || name.endsWith("index"))) {
        if (isYear(file.getParentFile().getName())) {
          return Optional.of(file.getParentFile());
        } else {
          return Optional.empty();
        }
      }
      return Optional.of(new File(file, file.getName()));
    }

    private static boolean isYear(String name) {
      if (name.length() < 4) {
        return false;
      }
      for (char c : name.substring(0, 3).toCharArray()) {
        if (!Character.isDigit(c)) {
          return false;
        }
      }
      return true;
    }

    public Range getIndexRange() {
      return indexRange;
    }

    public Range getTimeRange() {
      return timeRange;
    }

    public AbstractIterable<Log> iterate(final Query search) {
      return iterate(indexRange.start(), search);
    }

    public AbstractIterable<Log> iterate(final long startIndex, final Query search) {
      logger.debug("iterating logs from {}", startIndex);
      return new AbstractIterable<Log>() {
        long index = startIndex;
        Log last;
        long numLogs;

        @Override
        protected Log computeNext() {
          while (true) {
            Log log = getLog(index++);
            if (log != null && !log.isPaddedEntry()) {
              last = log;
              numLogs++;
              return log;
            } else if (log == null) {
              // at the end
              index = indexRange.stop() + 1;
              logger.debug("found {} last {}", numLogs, last);
              return null;
            }
            // padded entry
          }
        }
      };
    }

    Log binarySearchAfterOrEqualTime(long startTime, long lastWritten) {
      long low = indexRange.start();
      long high = lastWritten;
      long timestamp = 0;
      Log log = null;
      while (low < high) {
        long mid = (low + high) >>> 1;
        log = getLog(mid);
        if (log.isPaddedEntry()) {
          // padded entry
          high = mid - 1;
        } else {
          timestamp = log.getTimestamp();
          if (timestamp < startTime) {
            low = mid + 1;
          } else if (timestamp > startTime) {
            high = mid - 1;
          } else {
            break;
          }
        }
      }
      if (log == null) {
        Log lowLog = getLog(low);
        logger.debug("binarySearchAfterOrEqualTime lowLog found {}", lowLog);
        return lowLog;
      }

      Log found = adjust(log.getIndex(), timestamp, startTime);
      logger.debug("binarySearchAfterOrEqualTime found {}", found);
      return found;
    }

    private Log adjust(long index, long timestamp, long startTime) {
      // thousands of logs may have exact same milli timestamp
      // so adjust index to first non-padded index
      Log lastNonPaddedEntry = null;
      while (true) {
        if (timestamp >= startTime) {
          Log optional = getLog(--index);
          if (optional != null && !optional.isPaddedEntry() && optional.getTimestamp() == startTime) {
            lastNonPaddedEntry = optional;
          }
          if (optional == null || (!optional.isPaddedEntry() && optional.getTimestamp() < startTime)) {
            return lastNonPaddedEntry;
          }
        } else {
          return lastNonPaddedEntry;
        }
      }
    }

    public Log getLog(long index) {
      initalize();
      if (!indexRange.contains(index)) {
        logger.debug("indexRange {} notContains {}", indexRange, index);
        return null;
      }
      long localIndex = index - indexOffset;
      if (!tailer.index(localIndex)) {
        if (tailer.wasPadding()) {
          logger.debug("padded ", index);
          return Log.paddedEntry(localIndex, index);
        }
        logger.debug("chronicle {} notContain {} {}", basePath, index, localIndex);
        return null;
      }
      return new Log(localIndex, index, tailer);
    }

    public void close() throws IOException {
      if (chronicle != null) {
        chronicle.close();
      }
      if (tailer != null) {
        tailer.close();
      }
    }

    public long getLastWrittenIndex() {
      initalize();
      long index = chronicle.findTheLastIndex();
      return indexOffset + (index == -1 ? 0 : index);
    }

    private void initalize() {
      if (chronicle == null) {
        try {
          File file = new File(basePath);
          if (!file.getParentFile().getName().equals(file.getName())) {
            // log buffer basePath pointed to a specific range /tmp/logbuffer/2014-11-09-00-28-41-GMT
            file = new File(file, file.getName());
          }
          chronicle = new IndexedChronicle(file.toString(), config);
          tailer = chronicle.createTailer();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public String toString() {
      return "path='" + basePath + '\'' +
        ", idx=" + indexRange.start() +
        ", time=" + timeRange.start();
    }
  }

  static class LogIterator extends AbstractIterable<Log> {
    Query query;
    Dir dir;
    Log last;
    boolean foundFirst = false;
    AbstractIterable<Log> logs;
    AbstractIterable<Dir> dirs;

    public LogIterator(Dirs dirs, Query query) {
      this.dirs = dirs.execute(query);
      this.query = query;
    }

    public LogIterator(Dir dir) {
      this.dir = dir;
      this.query = Query.closedIndex(dir.indexRange.start(), dir.indexRange.stop());
    }


    @Override
    protected Log computeNext() {
      while (true) {
        if (dir == null && dirs != null) {
          dir = dirs.computeNext();
        }
        if (dir == null) {
          // no more directories
          return null;
        }
        // time queries are special because startIndex on FIRST range ONLY
        // must be found quickly, after that simply iterate til end of query
        if (logs == null && query.isTimeQuery() && !foundFirst) {
          long firstIndex = findFirstIndex(dir, query);
          logs = dir.iterate(firstIndex, query);
          foundFirst = true;
        } else if (logs == null && query.isIndexQuery() && !foundFirst) {
          long startIndex = query.start();
          if (startIndex < dir.getIndexRange().start()) {
            startIndex = dir.getIndexRange().start();
          }
          logs = dir.iterate(startIndex, query);
          foundFirst = true;
        } else if (logs == null) {
          logs = dir.iterate(query);
        }
        Log log = logs.computeNext();
        if (log != null && log.isIn(query)) {
          last = log;
          return log;
        } else if (log != null && log.greaterThan(query)) {
          // no more matching logs
          logs = null;
          dir = null;
        } else if (log == null) {
          // no more logs in dir
          logs = null;
          dir = null;
        }
      }
    }
    public Log getLastProcessed() {
      return last;
    }
  }

  public static long findFirstIndex(Dir dir, Query search) {
    long startIndex = dir.indexRange.start();
    if (search.isTimeQuery()) {
      final long lastWrittenIndex = dir.getLastWrittenIndex();
      Log log = dir.binarySearchAfterOrEqualTime(search.start(), lastWrittenIndex);
      if (log != null) {
        startIndex = log.getIndex();
      }
    }
    return startIndex;
  }

}
