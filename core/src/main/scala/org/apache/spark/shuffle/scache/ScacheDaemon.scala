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

import java.net.Inet4Address

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.util.Utils

/**
  * Created by frankfzw on 17-3-14.
  */
private[spark] class ScacheDaemon (
    conf: SparkConf,
    rpcEnv: RpcEnv) extends Logging {

  val scacheHome = conf.get("scache.home", "/root/SCache")
  val scachePort = conf.getInt("scache.client.port", 5678)
  val clientRef = setupClientRef(scachePort)

  private def setupClientRef(port: Int): RpcEndpointRef = {
    val localIP = Utils.localHostName()
    val hostName = Utils.localHostName()
    val rpcAddr = new RpcAddress(localIP, port)
    rpcEnv.setupEndpointRef("scache.client", rpcAddr, "ScacheClient")
  }

}

