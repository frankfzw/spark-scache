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


import java.net.InetAddress

import com.google.common.net.InetAddresses
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.scheduler.MapStatus
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

  val reduceStatus = new ConcurrentHashMap[(Int, Int), mutable.HashMap[Int, Array[String]]]()

  private var runningJId: Int = -1

  def setRunningJId(jid: Int, shuffleId: Int, reduceNum: Int): Unit = {
    logDebug(s"Update jid from $runningJId to $jid")
    runningJId = jid
    daemon.mapStart(jid, shuffleId, reduceNum)
  }

  def setRunningJId(jid: Int): Unit = {
    logDebug(s"Update jid from $runningJId to $jid by driver")
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
    getShuffleStatusInternal(jobId, shuffleId)
  }

  def getShuffleStatus(shuffleId: Int): mutable.HashMap[Int, Array[String]] = {
    getShuffleStatusInternal(runningJId, shuffleId)
  }

  def getShuffleStatusForPartition(shuffleId: Int, reduceId: Int): Array[String] = {
    val statuses = getShuffleStatusInternal(runningJId, shuffleId)
    val ret = statuses.get(reduceId).getOrElse(new Array[String](0))
    ret
  }

  private def getShuffleStatusInternal
    (jobId: Int, shuffleId: Int): mutable.HashMap[Int, Array[String]] = {
    val key = (jobId, shuffleId)
    if (reduceStatus.contains(key)) {
      return reduceStatus.get(key)
    } else {
      val res = daemon.getShuffleStatus(jobId, shuffleId)
      val str = res.values.head(0)
      if (InetAddresses.isInetAddress(str)) {
        val newAddr = new mutable.HashMap[Int, Array[String]]
        for ((k, v) <- res) {
          val addr = v.map(x => InetAddress.getByName(x).getHostName)
          newAddr.put(k, addr)
        }
        reduceStatus.putIfAbsent(key, newAddr)
        return newAddr
      } else {
        reduceStatus.putIfAbsent(key, res)
      }
      return res
    }
  }

  def mapEnd
    (jobId: Int, shuffleId: Int, mapId: Int, reduce: Int, status: MapStatus): Unit = {
    val sizes = new Array[Long](reduce)
    for (i <- 0 until reduce) {
      sizes(i) = status.getSizeForBlock(i)
    }
    daemon.mapEnd(jobId, shuffleId, mapId, sizes)
  }

  def mapEnd(shuffleId: Int, mapId: Int, reduce: Int, status: MapStatus): Unit = {
    mapEnd(runningJId, shuffleId, mapId, reduce, status)
  }

  def stop(): Unit = {
    daemon.stop()
  }
}

private[spark] object ScacheDaemon {
  val ENDPOINT_NAME = "ScacheClient"
}

