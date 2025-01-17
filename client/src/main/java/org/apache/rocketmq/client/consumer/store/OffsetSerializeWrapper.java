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
package org.apache.rocketmq.client.consumer.store;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * Wrapper class for offset serialization
 * OffsetSerializeWrapper中同样使用了ConcurrentMap，从磁盘的offsets.json文件中读取数据后，将JSON转为OffsetSerializeWrapper对象
 * ，就可以通过OffsetSerializeWrapper的offsetTable获取到之前保存的每个消息队列的消费进度，然后加入到LocalFileOffsetStore的offsetTable中
 */
public class OffsetSerializeWrapper extends RemotingSerializable {
    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable =
        new ConcurrentHashMap<MessageQueue, AtomicLong>();

    public ConcurrentMap<MessageQueue, AtomicLong> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentMap<MessageQueue, AtomicLong> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
