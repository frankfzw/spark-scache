/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.spark.shuffle.scache

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}
import java.nio.channels.FileChannel

import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream
import org.apache.spark.Logging
import org.apache.spark.serializer.{SerializationStream, SerializerInstance}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.storage.{BlockId, DiskBlockObjectWriter, FileSegment, TimeTrackingOutputStream}
import org.apache.spark.util.Utils

/**
  * Created by frankfzw on 4/23/2017.
  */
class ScacheBlockObjectWriter (
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    compressStream: OutputStream => OutputStream,
    syncWrites: Boolean,
    writeMetrics: ShuffleWriteMetrics,
    blockId: BlockId = null,
    daemon: ScacheDaemon)
  extends DiskBlockObjectWriter(new File("/tmp/fake"), serializerInstance, bufferSize,
      compressStream, syncWrites, writeMetrics, blockId, daemon) with Logging{

  private var bos: ByteOutputStream = null
  private var bs: OutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private var initialized = false
  private var hasBeenClosed = false
  private var commitAndCloseHasBeenCalled = false

  private val initialPosition = 0
  private var finalPosition: Long = -1
  private var reportedPosition = initialPosition
  private var numRecordsWritten = 0

  override def open(): DiskBlockObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    bos = new ByteOutputStream()
    ts = new TimeTrackingOutputStream(writeMetrics, bos)
    bs = compressStream(new BufferedOutputStream(ts, bufferSize))
    objOut = serializerInstance.serializeStream(bs)
    initialized = true
    this
  }

  override def close(): Unit = {
    if (initialized) {
      Utils.tryWithSafeFinally {
        if (syncWrites) {
          // Force outstanding writes to disk and track how long it takes
          objOut.flush()
          val start = System.nanoTime()
          writeMetrics.incShuffleWriteTime(System.nanoTime() - start)
        }
      } {
        objOut.close()
      }
      daemon.putBlock(blockId, bos.getBytes, bos.size(), bos.size())

      bs = null
      bos = null
      ts = null
      objOut = null
      initialized = false
      hasBeenClosed = true
    }
  }

  override def isOpen(): Boolean = objOut != null

  override def commitAndClose(): Unit = {
    if (initialized) {
      objOut.flush()
      bs.flush()
      close()
      finalPosition = bos.size()
      // In certain compression codecs, more bytes are written after close() is called
      writeMetrics.incShuffleBytesWritten(finalPosition - reportedPosition)
    } else {
      finalPosition = bos.size()
    }
    commitAndCloseHasBeenCalled = true
  }

  override def revertPartialWritesAndClose(): File = {
    try {
      if (initialized) {
        writeMetrics.decShuffleBytesWritten(reportedPosition - initialPosition)
        writeMetrics.decShuffleRecordsWritten(numRecordsWritten)
        objOut.flush()
        bs.flush()
        close()
      }
      null

    } catch {
      case e: Exception =>
        logError("Uncaught exception while reverting partial writes ", e)
        null
    }
  }

  override def write(key: Any, value: Any): Unit = {
    if (!initialized) {
      open()
    }

    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!initialized) {
      open()
    }

    bs.write(kvBytes, offs, len)
  }

  override def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incShuffleRecordsWritten(1)

    if (numRecordsWritten % 32 == 0) {
      updateBytesWritten()
    }
  }

  override def fileSegment(): FileSegment = {
    if (!commitAndCloseHasBeenCalled) {
      throw new IllegalStateException(
        "fileSegment() is only valid after commitAndClose() has been called")
    }
    // just fake a file to report length
    val f = new File("/tmp/fake")
    new FileSegment(f, initialPosition, finalPosition - initialPosition)
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private def updateBytesWritten() {
    val pos = bos.size()
    writeMetrics.incShuffleBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }

  // For testing
  private[spark] override def flush() {
    objOut.flush()
    bs.flush()
  }
}
