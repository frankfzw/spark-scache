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


import org.apache.spark.storage.{BlockId, ScacheBlockId, ShuffleBlockId}
import org.apache.spark.{Logging, ShuffleDependency, SparkConf}
import org.scache.deploy.Daemon

import scala.collection.mutable

/**
  * Created by frankfzw on 17-3-14.
  */
private[spark] class ScacheDaemon (conf: SparkConf) extends Logging {

  val scacheHome = conf.get("spark.scache.home", "~/SCache")
  val platform = "spark"

  val daemon = new Daemon(scacheHome, platform)

  private var runningJId: Int = -1

  def setRunningJId(jid: Int): Unit = {
    logDebug(s"Update jid from $runningJId to $jid")
    runningJId = jid
  }

  def getRunningJId: Int = runningJId

  // test
  // putBlock(0, ShuffleBlockId(0, 0, 0), Array[Byte](2), 2, 2)

  def putBlock
    (blockId: BlockId, data: Array[Byte], rawLen: Int, compressedLen: Int): Boolean = {
    if (!blockId.isShuffle) {
      logError(s"Unexpected block type, excepted ${ShuffleBlockId.getClass.getSimpleName}" +
        s"got ${blockId.getClass.getSimpleName}")
      return false
    }
    val bId = blockId.asInstanceOf[ShuffleBlockId]
    val scacheBlockId = ScacheBlockId("spark", runningJId, bId.shuffleId, bId.mapId, bId.reduceId)
    daemon.putBlock(scacheBlockId.toString, data, rawLen, compressedLen)
    return true
  }

  def getBlock(blockId: BlockId): Option[Array[Byte]] = {
    if (!blockId.isShuffle) {
      logError(s"Unexpected block type, excepted ${ShuffleBlockId.getClass.getSimpleName}" +
        s"got ${blockId.getClass.getSimpleName}")
      return None
    }
    val bId = blockId.asInstanceOf[ShuffleBlockId]
    val scacheBlockId = ScacheBlockId("spark", runningJId, bId.shuffleId, bId.mapId, bId.reduceId)
    daemon.getBlock(scacheBlockId.toString)
  }

  def registerShuffles
    (jobId: Int, shuffleIds: Array[Int], maps: Array[Int], reduces: Array[Int]): Unit = {
    daemon.registerShuffles(jobId, shuffleIds, maps, reduces)
  }

  def registerShuffles
    (jobId: Int, shuffleDeps: Array[ShuffleDependency[_, _, _]], reduces: Int): Unit = {
    val shuffleIds = shuffleDeps.map(x => x.shuffleId)
    val maps = shuffleDeps.map(x => x.rdd.partitions.length)
    val reducesArr = Array(maps.length).map(x => reduces)
    daemon.registerShuffles(jobId, shuffleIds, maps, reducesArr)
  }

  def getShuffleStatus(jobId: Int, shuffleId: Int): mutable.HashMap[Int, Array[String]] = {
    daemon.getShuffleStatus(jobId, shuffleId)
  }

  def mapEnd(jobId: Int, shuffleId: Int, mapId: Int): Unit = {
    daemon.mapEnd(jobId, shuffleId, mapId)
  }

  def stop(): Unit = {
    daemon.stop()
  }
}

private[spark] object ScacheDaemon {
  val ENDPOINT_NAME = "ScacheClient"
}

