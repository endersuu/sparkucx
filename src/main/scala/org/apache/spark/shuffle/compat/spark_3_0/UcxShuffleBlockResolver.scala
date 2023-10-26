/*
 * Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.compat.spark_3_0

import org.apache.spark.network.shuffle.ExecutorDiskUtils
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.shuffle.{CommonUcxShuffleBlockResolver, CommonUcxShuffleManager}
import org.apache.spark.storage.ShuffleIndexBlockId
import org.apache.spark.{SparkEnv, TaskContext}

import java.io.{File, RandomAccessFile}

/** Mapper entry point for UcxShuffle plugin. Performs memory registration
  * of data and index files and publish addresses to driver metadata buffer.
  */
class UcxShuffleBlockResolver(ucxShuffleManager: CommonUcxShuffleManager)
    extends CommonUcxShuffleBlockResolver(ucxShuffleManager) {

  override def writeIndexFileAndCommit(
      shuffleId: ShuffleId,
      mapId: Long,
      lengths: Array[Long],
      dataTmp: File
  ): Unit = {
    super.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)

    // In Spark-3.0 MapId is long and unique among all jobs in spark. We need to use mapPartitionId as offset
    // in metadata buffer
    val mapPartitionId = TaskContext.getPartitionId()
    val dataFile = getDataFile(shuffleId, mapId)
    val dataBackFile = new RandomAccessFile(dataFile, "rw")

    logInfo(
      s"==> writeIndexFileAndCommit, shuffleId=$shuffleId, mapId=$mapId, mapPartitionId=$mapPartitionId, "
        + s"lengths=${lengths.mkString("[", ", ", "]")}"
    )

    if (dataBackFile.length() == 0) {
      dataBackFile.close()
      return
    }

    val indexFile = getIndexFile(shuffleId, mapId)
    val indexBackFile = new RandomAccessFile(indexFile, "rw")

    writeIndexFileAndCommitCommon(
      shuffleId,
      mapPartitionId,
      lengths,
      indexBackFile,
      dataBackFile
    )
  }

  private def getIndexFile(
      shuffleId: Int,
      mapId: Long,
      dirs: Option[Array[String]] = None
  ): File = {
    val blockId = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    val blockManager = SparkEnv.get.blockManager
    dirs
      .map(
        ExecutorDiskUtils
          .getFile(_, blockManager.subDirsPerLocalDir, blockId.name)
      )
      .getOrElse(blockManager.diskBlockManager.getFile(blockId))
  }
}
