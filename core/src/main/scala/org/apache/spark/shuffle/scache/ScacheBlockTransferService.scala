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

import java.nio.ByteBuffer

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.Utils

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

/**
  * Created by frankfzw on 17-4-25.
  */
class ScacheBlockTransferService(daemon: ScacheDaemon) extends Logging{

  /**
    * Fetch a sequence of blocks from a remote node asynchronously,
    *
    * Note that this API takes a sequence so the implementation can batch requests, and does not
    * return a future so the underlying implementation can invoke onBlockFetchSuccess as soon as
    * the data of a block is fetched, rather than waiting for all blocks to be fetched.
    */
  def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener): Unit = {
    for (bid <- blockIds) {
      val thread = new Thread {
        override def run: Unit = {
          daemon.getBlock(BlockId.apply(bid)) match {
            case Some(data) =>
              val ret = ByteBuffer.allocate(data.size)
              ret.put(data)
              ret.flip()
              listener.onBlockFetchSuccess(bid, new NioManagedBuffer(ret))
            case None =>
              listener.onBlockFetchFailure(bid, new Throwable(s"${bid} not found on SCache"))
          }
        }
      }
      thread.start()
    }
  }


  /**
    * A special case of [[fetchBlocks]], as it fetches only one block
    * and is blocking.
    *
    */
  def fetchBlockSync (host: String, port: Int, execId: String, blockId: String): ManagedBuffer = {
    // A monitor for the thread to wait on.
    val result = Promise[ManagedBuffer]()
    fetchBlocks(host, port, execId, Array(blockId),
      new BlockFetchingListener {
        override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
          result.failure(exception)
        }
        override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
          val ret = ByteBuffer.allocate(data.size.toInt)
          ret.put(data.nioByteBuffer())
          ret.flip()
          result.success(new NioManagedBuffer(ret))
        }
      })

    Await.result(result.future, Duration.Inf)
  }


}
