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

import java.io.File
import java.net.Inet4Address
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.shuffle.scache.DeployMessage.PutBlock
import org.apache.spark.storage.ScacheBlockId
import org.apache.spark.util.{ThreadUtils, Utils}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by frankfzw on 17-3-14.
  */
private[spark] class ScacheDaemon (
    conf: SparkConf,
    rpcEnv: RpcEnv) extends Logging {

  private val asyncThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("scache-daemon-async-thread-pool")
  private implicit val asyncExecutionContext = ExecutionContext.fromExecutorService(asyncThreadPool)

  val scacheHome = conf.get("scache.home", "/root/SCache")
  val scachePort = conf.getInt("scache.client.port", 5678)
  val scacheRpcAddr = new RpcAddress(Utils.localHostName(), scachePort)
  val endpointRef = rpcEnv.setupEndpointRef("scache.client",
    scacheRpcAddr, ScacheDaemon.ENDPOINT_NAME)

  // test
  // putBlock(0, 0, 0, 0, new Array[Byte](5), 5)

  def putBlock(jobId: Int, shuffleId: Int, mapId: Int, reduceId: Int,
               data: Array[Byte], len: Int): Unit = {
    val blockId = new ScacheBlockId("spark", jobId, shuffleId, mapId, reduceId)
    doAsync[Boolean]("Put Block to Scache")(putBlockInternal(blockId, data, len))
  }

  private def putBlockInternal(blockId: ScacheBlockId, data: Array[Byte], len: Int): Boolean = {
    val path = scacheHome + "/tmp/" + blockId.toString()
    val f = new File(path)
    try {
      val channel = FileChannel.open(f.toPath,
        StandardOpenOption.READ, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      val buf = channel.map(FileChannel.MapMode.READ_WRITE, 0, len)
      buf.put(data)
      endpointRef.askWithRetry[Boolean](new PutBlock(blockId, len))
    }
  }

  private def doAsync[T](actionMessage: String)(body: => T) {
    val future = Future {
      logDebug(actionMessage)
      body
    }
    future.onSuccess { case response =>
      logDebug("Done " + actionMessage + ", response is " + response)
    }
    future.onFailure { case t: Throwable =>
      logError("Error in " + actionMessage, t)
    }
  }

  private def setupScacheClientRef(
     systemName: String,
     address: RpcAddress,
     endpointName: String): RpcEndpointRef = {
    val uri = rpcEnv.uriOf(systemName, address, endpointName)
    val addr = RpcEndpointAddress(uri)
  }

}

private[spark] object ScacheDaemon {
  val ENDPOINT_NAME = "ScacheClient"
}

