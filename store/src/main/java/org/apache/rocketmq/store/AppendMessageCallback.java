/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import org.apache.rocketmq.common.message.MessageExtBatch;

/**
 * Write messages callback interface
 */
public interface AppendMessageCallback {

    /**
     * After message serialization, write MapedByteBuffer(序列化之后写入消息)
     * fileFromOffset：文件的起始位置偏移量
     * byteBuffer：缓冲区，也就是上一步中创建的共享内存区
     * maxBlank：上一步中可知传入的是文件总大小减去当前要写入的位置，也就是文件剩余空间大小
     * msgInner：消息内容的封装体
     * putMessageContext：消息写入上下文
     * @return How many bytes to write
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
        final int maxBlank, final MessageExtBrokerInner msg);

    /**
     * After batched message serialization, write MapedByteBuffer
     *
     * @param messageExtBatch, backed up by a byte array
     * @return How many bytes to write
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
        final int maxBlank, final MessageExtBatch messageExtBatch);
}
