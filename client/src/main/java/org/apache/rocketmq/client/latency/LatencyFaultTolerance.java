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

package org.apache.rocketmq.client.latency;

/**
 * 生产者负载均衡核心入口类
 * @param <T>
 */
public interface LatencyFaultTolerance<T> {
    void updateFaultItem(final T name, final long currentLatency, final long notAvailableDuration);

    /**
     * 判断达到退避时间，其实就是根据之前延时时间绝定的不可用时间是否达到了。
     * @param name
     * @return
     */
    boolean isAvailable(final T name);

    void remove(final T name);

    /**
     * 选择最新的一个队列
     * @return
     */
    T pickOneAtLeast();
}
