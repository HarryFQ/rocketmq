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
     *
     * @param name
     * @return
     */
    boolean isAvailable(final T name);

    void remove(final T name);

    /**
     * 选择最新的一个broker
     *
     * 在选择消息队列时，如果开启故障延迟机制并且未找到合适的消息队列，会调用pickOneAtLeast方法选择一个Broker，那么是如何选择Broker的呢？
     *
     * 1. 首先遍历faultItemTableMap集合，将每一个Broker对应的FaultItem加入到LinkedList链表中
     *
     * 2. 调用sort方法对链表进行排序，默认是正序从小到大排序，FaultItem还实现Comparable就是为了在这里进行排序，值小的排在链表前面
     *
     * 3. 计算中间值half：
     *
     *  3.1如果half值小于等于0，取链表中的第一个元素
     *  3.2如果half值大于0，从前half个元素中轮询选择元素
     *由FaultItem的compareTo方法可知，currentLatency和startTimestamp的值越小，整个FaultItem的值也就越小，正序排序时越靠前
     * ，靠前表示向Broker发送消息的延迟越低，在选择Broker时优先级越高，所以如果half值小于等于0的时候，取链表中的第一个元素，half值大于0的时候
     * ，处于链表前half个的Brokerddd，延迟都是相对较低的，此时轮询从前haft个Broker中选择一个Broker。
     * @return
     */
    T pickOneAtLeast();
}
