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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Keeps track of the index for a specific reader or writer.
 */
final class Index {
  private SingleMappedFileCache fileCache;
  private ByteBuffer buffer;

  Index(String filePath) throws IOException {
    fileCache = new SingleMappedFileCache(filePath, 16);
    buffer = fileCache.acquireBuffer(0, false);
  }

  synchronized void writeIndex(long index) throws IOException {
    buffer.putLong(0, index);
  }

  synchronized long getIndex() throws IOException {
    return buffer.getLong(0);
  }

  synchronized void writeTimestamp(long nanos) {
    buffer.putLong(8, nanos);
  }

  synchronized long getTimestamp() throws IOException {
    return buffer.getLong(8);
  }

  void close() throws IOException {
    fileCache.close();
  }

  long getAndIncrement() throws IOException {
    long index = getIndex();
    writeIndex(index + 1);
    writeTimestamp(System.currentTimeMillis());
    return index;
  }
}
