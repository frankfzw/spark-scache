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

import org.apache.spark.storage.BlockId

/**
  * Created by frankfzw on 17-3-14.
  */

private[spark] trait DeployMessage extends Serializable

private[spark] object DeployMessage {
  sealed trait FromDaemon

  case class PutBlock(scacheBlockId: BlockId, size: Int) extends FromDaemon

  case class GetBlock(scacheBlockId: BlockId) extends FromDaemon

  case class RegisterShuffle(appName: String,
                             jobId: Int,
                             shuffleId: Int,
                             numMapTask: Int,
                             numReduceTask: Int) extends FromDaemon

  case class MapEnd(appName: String, jobId: Int, shuffleId: Int, mapId: Int) extends FromDaemon

  case class GetShuffleStatus(appName: String, jobId: Int, shuffleId: Int) extends FromDaemon
}
