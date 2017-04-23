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

package org.apache.spark.storage

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}
import java.nio.channels.FileChannel

import org.apache.spark.Logging
import org.apache.spark.serializer.{SerializationStream, SerializerInstance}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.shuffle.scache.ScacheDaemon
import org.apache.spark.util.Utils

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block and can guarantee atomicity in the case of faults as it allows the caller to
 * revert partial writes.
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 */
private[spark] abstract class DiskBlockObjectWriter(
    val file: File,
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    compressStream: OutputStream => OutputStream,
    syncWrites: Boolean,
    // These write metrics concurrently shared with other active DiskBlockObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetrics,
    val blockId: BlockId = null,
    val daemon: ScacheDaemon = null) extends OutputStream{
  def open(): DiskBlockObjectWriter
  override def close(): Unit
  def isOpen(): Boolean
  def commitAndClose(): Unit
  def revertPartialWritesAndClose(): File
  def write(key: Any, value: Any): Unit
  override def write(b: Int): Unit
  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit
  def recordWritten(): Unit
  def fileSegment(): FileSegment
}


