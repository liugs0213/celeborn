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

package org.apache.celeborn.client.read;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.client.read.checkpoint.PartitionReaderCheckpointMetadata;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbBufferStreamEnd;
import org.apache.celeborn.common.protocol.PbOpenStream;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.protocol.StreamType;
import org.apache.celeborn.common.util.FileChannelUtils;
import org.apache.celeborn.common.util.ThreadUtils;

public class LocalPartitionReader implements PartitionReader {

  private static final Logger logger = LoggerFactory.getLogger(LocalPartitionReader.class);
  private final PartitionLocation location;
  private int returnedChunks = 0;
  private int chunkIndex = 0;
  private String fullPath;
  private FileChannel shuffleChannel;
  private List<Long> chunkOffsets;
  private PbStreamHandler streamHandler;
  private TransportClient client;
  private MetricsCallback metricsCallback;
  private int startChunkIndex;
  private int endChunkIndex;

  @SuppressWarnings("StaticAssignmentInConstructor")
  public LocalPartitionReader(
          CelebornConf conf,
          String shuffleKey,
          PartitionLocation location,
          PbStreamHandler pbStreamHandler,
          TransportClientFactory clientFactory,
          int startMapIndex,
          int endMapIndex,
          MetricsCallback metricsCallback,
          int startChunkIndex,
          int endChunkIndex)
          throws IOException {
    this.location = location;
    long fetchTimeoutMs = conf.clientFetchTimeoutMs();
    this.metricsCallback = metricsCallback;
    try {
      client = clientFactory.createClient(location.getHost(), location.getFetchPort(), 0);
      if (pbStreamHandler == null) {
        TransportMessage openStreamMsg =
            new TransportMessage(
                MessageType.OPEN_STREAM,
                PbOpenStream.newBuilder()
                    .setShuffleKey(shuffleKey)
                    .setFileName(location.getFileName())
                    .setStartIndex(startMapIndex)
                    .setEndIndex(endMapIndex)
                    .setReadLocalShuffle(conf.enableReadLocalShuffleFile())
                    .build()
                    .toByteArray());
        ByteBuffer response = client.sendRpcSync(openStreamMsg.toByteBuffer(), fetchTimeoutMs);
        streamHandler = TransportMessage.fromByteBuffer(response).getParsedPayload();
      } else {
        streamHandler = pbStreamHandler;
      }
      this.startChunkIndex = startChunkIndex == -1 ? 0 : startChunkIndex;
      this.endChunkIndex =
          endChunkIndex == -1
              ? streamHandler.getNumChunks() - 1
              : Math.min(streamHandler.getNumChunks() - 1, endChunkIndex);
      this.chunkIndex = this.startChunkIndex;
    } catch (IOException | InterruptedException e) {
      throw new IOException(
          "Read shuffle file from local file failed, partition location: "
              + location
              + " filePath: "
              + location.getStorageInfo().getFilePath(),
          e);
    }

    chunkOffsets = new ArrayList<>(streamHandler.getChunkOffsetsList());
    fullPath = streamHandler.getFullPath();

    logger.debug(
        "Local partition reader {} offsets:{}",
        location.getStorageInfo().getFilePath(),
        StringUtils.join(chunkOffsets, ","));

    ShuffleClient.incrementLocalReadCounter();
  }

  @Override
  public boolean hasNext() {
    logger.debug(
        "Check has next current index: {} chunks {}",
        returnedChunks,
        endChunkIndex - startChunkIndex + 1);
    return returnedChunks < endChunkIndex - startChunkIndex + 1;
  }

  @Override
  public ByteBuf next() throws IOException, InterruptedException {
    if (shuffleChannel == null) {
      shuffleChannel = FileChannelUtils.openReadableFileChannel(fullPath);
    }
    long offset = chunkOffsets.get(chunkIndex);
    long length = chunkOffsets.get(chunkIndex + 1) - offset;
    shuffleChannel.position(offset);
    logger.debug("Read {} offset {} length {}", chunkIndex, offset, length);
    // A chunk must be smaller than INT.MAX_VALUE
    ByteBuffer buffer = ByteBuffer.allocate((int) length);
    Long startFetchWait = System.nanoTime();
    while (buffer.hasRemaining()) {
      if (-1 == shuffleChannel.read(buffer)) {
        throw new CelebornIOException(
                "Read local file " + location.getStorageInfo().getFilePath() + " failed");
      }
    }
    metricsCallback.incReadTime(
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait));
    buffer.flip();
    chunkIndex++;
    returnedChunks++;
    return Unpooled.wrappedBuffer(buffer);
  }

  @Override
  public void close() {
    try {
      if (shuffleChannel != null) {
        shuffleChannel.close();
      }
    } catch (IOException e) {
      logger.warn("Close local shuffle file failed.", e);
    }
    closeStream();
  }

  private void closeStream() {
    if (client != null && client.isActive()) {
      TransportMessage bufferStreamEnd =
          new TransportMessage(
              MessageType.BUFFER_STREAM_END,
              PbBufferStreamEnd.newBuilder()
                  .setStreamType(StreamType.ChunkStream)
                  .setStreamId(streamHandler.getStreamId())
                  .build()
                  .toByteArray());
      client.sendRpc(bufferStreamEnd.toByteBuffer());
    }
  }

  @Override
  public PartitionLocation getLocation() {
    return location;
  }

  @Override
  public Optional<PartitionReaderCheckpointMetadata> getPartitionReaderCheckpointMetadata() {
    // TODO implement similar to {@link WorkerPartitionReader}
    return Optional.empty();
  }
}