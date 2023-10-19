/*
 * Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle

import org.apache.spark.SparkException
import org.apache.spark.shuffle.ucx.UnsafeUtils
import org.openucx.jucx.ucp.{UcpMemMapParams, UcpMemory, UcpRequest}
import org.openucx.jucx.{UcxCallback, UcxUtils}

import java.io.RandomAccessFile
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}
import scala.collection.JavaConverters._

/** Mapper entry point for UcxShuffle plugin. Performs memory registration
  * of data and index files and publish addresses to driver metadata buffer.
  */
abstract class CommonUcxShuffleBlockResolver(
    ucxShuffleManager: CommonUcxShuffleManager
) extends IndexShuffleBlockResolver(ucxShuffleManager.conf) {
  private lazy val memPool = ucxShuffleManager.ucxNode.getMemoryPool

  // Keep track of registered memory regions to release them when shuffle not needed
  private val fileMappings =
    new ConcurrentHashMap[ShuffleId, CopyOnWriteArrayList[UcpMemory]].asScala
  private val indexMappings =
    new ConcurrentHashMap[ShuffleId, CopyOnWriteArrayList[UcpMemory]].asScala

  /** Mapper commit protocol extension. Register index and data files and publish all needed
    * metadata to driver.
    */
  def writeIndexFileAndCommitCommon(
      shuffleId: ShuffleId,
      mapPartitionId: Int,
      lengths: Array[Long],
      indexBackFile: RandomAccessFile,
      dataBackFile: RandomAccessFile
  ): Unit = {

    assume(
      indexBackFile.length() == UnsafeUtils.LONG_SIZE * (lengths.length + 1)
    )

    val startTime = System.currentTimeMillis()

    fileMappings.putIfAbsent(shuffleId, new CopyOnWriteArrayList[UcpMemory]())
    indexMappings.putIfAbsent(shuffleId, new CopyOnWriteArrayList[UcpMemory]())

    // Memory map and register data and index file.
    def mapFile(backFile: RandomAccessFile): UcpMemory = {

      val fileChannel = backFile.getChannel
      val address = UnsafeUtils.mmap(fileChannel, 0, backFile.length())
      val memMapParams = new UcpMemMapParams()
        .setAddress(address)
        .setLength(backFile.length())
      if (ucxShuffleManager.ucxShuffleConf.useOdp) {
        memMapParams.nonBlocking()
      }

      backFile.close()
      fileChannel.close()

      ucxShuffleManager.ucxNode.getContext.memoryMap(memMapParams)
    }

    val indexMemory = mapFile(indexBackFile)
    val dataMemory = mapFile(dataBackFile)

    indexMappings(shuffleId).add(indexMemory)
    fileMappings(shuffleId).add(dataMemory)

    val indexMemoryRKey = indexMemory.getRemoteKeyBuffer
    val fileMemoryRKey = dataMemory.getRemoteKeyBuffer

    val metadataRegisteredMemory =
      memPool.get(fileMemoryRKey.capacity() + indexMemoryRKey.capacity() + 24)
    val metadataBuffer = metadataRegisteredMemory.getBuffer.slice()

    if (
      metadataBuffer
        .remaining() > ucxShuffleManager.ucxShuffleConf.metadataBlockSize
    ) {
      throw new SparkException(
        s"Metadata block size ${metadataBuffer.remaining() / 2} " +
          s"is greater then configured ${ucxShuffleManager.ucxShuffleConf.RKEY_SIZE.key}" +
          s"(${ucxShuffleManager.ucxShuffleConf.metadataBlockSize})."
      )
    }

    metadataBuffer.clear()

    metadataBuffer.putLong(indexMemory.getAddress)
    metadataBuffer.putLong(dataMemory.getAddress)

    metadataBuffer.putInt(indexMemoryRKey.capacity())
    metadataBuffer.put(indexMemoryRKey)

    metadataBuffer.putInt(fileMemoryRKey.capacity())
    metadataBuffer.put(fileMemoryRKey)

    metadataBuffer.clear()

    val workerWrapper = ucxShuffleManager.ucxNode.getThreadLocalWorker
    val driverMetadata = workerWrapper.getDriverMetadata(shuffleId)
    val driverOffset = driverMetadata.address +
      mapPartitionId * ucxShuffleManager.ucxShuffleConf.metadataBlockSize

    val driverEndpoint = workerWrapper.driverEndpoint
    val request = driverEndpoint.putNonBlocking(
      UcxUtils.getAddress(metadataBuffer),
      metadataBuffer.remaining(),
      driverOffset,
      driverMetadata.driverRkey,
      new UcxCallback {
        override def onSuccess(request: UcpRequest): Unit =
          logInfo(
            s"ucx shuffle write done: mapPartitionId=$mapPartitionId, " +
              s"overhead of registering files and publishing takes ${System.currentTimeMillis() - startTime} ms"
          )
      }
    )

    workerWrapper.preconnect()
    // Blocking progress needed to make sure last mapper published data to driver before
    // reducer starts.
    workerWrapper.waitRequest(request)
    memPool.put(metadataRegisteredMemory)
  }

  override def stop(): Unit = {
    fileMappings.keys.foreach(removeShuffle)
  }

  def removeShuffle(shuffleId: Int): Unit = {
    fileMappings
      .remove(shuffleId)
      .foreach((mappings: CopyOnWriteArrayList[UcpMemory]) =>
        mappings.asScala.par.foreach(unregisterAndUnmap)
      )
    indexMappings
      .remove(shuffleId)
      .foreach((mappings: CopyOnWriteArrayList[UcpMemory]) =>
        mappings.asScala.par.foreach(unregisterAndUnmap)
      )
  }

  private def unregisterAndUnmap(mem: UcpMemory): Unit = {
    val address = mem.getAddress
    val length = mem.getLength
    mem.deregister()
    UnsafeUtils.munmap(address, length)
  }
}
