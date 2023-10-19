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

import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.common.ThreadLocalIndex;

public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {
    /**
     * 记录了每个Broker对应的FaultItem，在updateFaultItem方法中首先根据Broker名称从faultItemTable获取FaultItem。
     * 如果获取为空，说明需要新增FaultItem，新建FaultItem对象，设置传入的currentLatency延迟时间（消息发送结束时间 - 开始时间）和开始时间即当前时间 +notAvailableDuration，notAvailableDuration值有两种情况，值为30000毫秒或者与currentLatency的值一致
     * 如果获取不为空，说明之前已经创建过对应的FaultItem，更新FaultItem中的currentLatency延迟时间和StartTimestamp开始时间
     */
    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<String, FaultItem>(16);

    private final ThreadLocalIndex whichItemWorst = new ThreadLocalIndex();

    /**
     * 更新FaultItem
     * @param name Broker名称
     * @param currentLatency 延迟时间，也就是发送消息耗时：请求结束时间 - 开始时间
     * @param notAvailableDuration 不可用的持续时间，也就是上一步中的duration
     */
    @Override
    public void updateFaultItem(final String name, final long currentLatency, final long notAvailableDuration) {
        // 获取FaultItem
        FaultItem old = this.faultItemTable.get(name);
        // 如果不存在就新建
        if (null == old) {
            // 新建FaultItem
            final FaultItem faultItem = new FaultItem(name);
            // 设置currentLatency延迟时间
            faultItem.setCurrentLatency(currentLatency);
            // 设置规避故障开始时间，当前时间 + 不可用的持续时间，不可用的持续时间有两种情况：值为30000或者与currentLatency一致
            faultItem.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
            // 添加到faultItemTable
            old = this.faultItemTable.putIfAbsent(name, faultItem);
            if (old != null) {
                old.setCurrentLatency(currentLatency);
                // 当前时间+隔离间隔时间
                old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
            }
        } else {
            // 更新时间
            old.setCurrentLatency(currentLatency);
            // 更新开始时间
            old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
        }
    }

    @Override
    public boolean isAvailable(final String name) {
        final FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            //调用FaultItem的isAvailable方法判断是否可用
            return faultItem.isAvailable();
        }
        return true;
    }

    @Override
    public void remove(final String name) {
        this.faultItemTable.remove(name);
    }

    @Override
    public String pickOneAtLeast() {
        final Enumeration<FaultItem> elements = this.faultItemTable.elements();
        List<FaultItem> tmpList = new LinkedList<FaultItem>();
        // 遍历faultItemTable
        while (elements.hasMoreElements()) {
            final FaultItem faultItem = elements.nextElement();
            // 将FaultItem添加到列表中
            tmpList.add(faultItem);
        }

        if (!tmpList.isEmpty()) {
            Collections.shuffle(tmpList);
            // 排序
            Collections.sort(tmpList);
            // 计算中间数
            final int half = tmpList.size() / 2;
            // 如果中位数小于等于0
            if (half <= 0) {
                // 获取第一个元素
                return tmpList.get(0).getName();
            } else {
                // 对中间数取余
                final int i = this.whichItemWorst.getAndIncrement() % half;
                return tmpList.get(i).getName();
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return "LatencyFaultToleranceImpl{" +
            "faultItemTable=" + faultItemTable +
            ", whichItemWorst=" + whichItemWorst +
            '}';
    }

    class FaultItem implements Comparable<FaultItem> {


        /**
         * Broker名称。
         */
        private final String name;
        /**
         * 延迟时间，等于发送消息耗时时间：发送消息结束时间 - 开始时间。
         * currentLatency的值越小表示FaultItem的值越小。currentLatency的值与Broker发送消息的耗时有关，耗时越低，值就越小
         */
        private volatile long currentLatency;
        /**
         * 规避故障开始时间：新建/更新FaultItem的时间 + 不可用的时间notAvailableDuration，notAvailableDuration值有两种情况，值为30000毫秒或者与currentLatency的值一致。
         * startTimestamp值越小同样表示整个FaultItem的值也越小。startTimestamp的值与currentLatency有关（值不为默认的30000毫秒情况下），currentLatency值越小，startTimestamp的值也越小。
         */
        private volatile long startTimestamp;

        public FaultItem(final String name) {
            this.name = name;
        }

        /**
         * 在排序的时候使用，对比大小的规则如下：
         *
         * 调用isAvailable方法判断当前对象和other的值是否相等，如果相等继续第2步，如果不相等，说明两个对象一个返回true一个返回false，此时优先判断当前对象的isAvailable方法返回值是否为true：
         *
         * true：表示当前对象比other小，返回-1，对应当前对象为true，other对象为false的情况。
         * false：调用other的isAvailable方法判断是否为true，如果为true，返回1，表示other比较大（对应当前对象为false，other对象为true的情况），否则继续第2步根据其他条件判断。
         * 对比currentLatency的值，如果currentLatency值小于other的，返回-1，表示当前对象比other小。
         *
         * 对比startTimestamp的值，如果startTimestamp值小于other的，返回-1，同样表示当前对象比other小。
         * @param other the object to be compared.
         * @return
         */
        @Override
        public int compareTo(final FaultItem other) {
            // 如果isAvailable不相等，说明一个为true一个为false
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable())// 如果当前对象为true
                    return -1; // 当前对象小

                if (other.isAvailable()// 如果other对象为true
                    return 1; // other对象大
            }

            // 对比发送消息耗时时间
            if (this.currentLatency < other.currentLatency)
                return -1;// 当前对象小
            else if (this.currentLatency > other.currentLatency) {
                return 1; // other对象大
            }
            // 对比故障规避开始时间
            if (this.startTimestamp < other.startTimestamp)
                return -1;
            else if (this.startTimestamp > other.startTimestamp) {
                return 1;
            }

            return 0;
        }

        /**
         * 用于开启故障延迟机制时判断Broker是否可用，可用判断方式为：当前时间 - startTimestamp的值大于等于 0，如果小于0则认为不可用。
         * startTimestamp的值为新建/更新FaultItem的时间 + 不可用的时间，如果当前时间减去规避故障开始时间的值大于等于0，说明此Broker已经超过了设置的规避时间，可以重新被选择用于发送消息
         *
         * @return true的时候表示FaultItem对象的值越小，因为true代表Broker已经过了规避故障的时间，可以重新被选择。
         * 所以故障延迟机制指的是在发送消息时记录每个Broker的耗时时间，如果某个Broker发生故障，但是生产者还未感知（NameServer 30s检测一次心跳
         * ，有可能Broker已经发生故障但未到检测时间，所以会有一定的延迟），用耗时时间做为一个故障规避时间（也可以是30000ms），此时消息会发送失败
         * ，在重试或者下次选择消息队列的时候，如果在规避时间内，可以避免再次选择到此Broker，以此达到故障规避的目的。
         */
        public boolean isAvailable() {
            // 判断当前时间是否超过退避时间
            return (System.currentTimeMillis() - startTimestamp) >= 0;
        }

        @Override
        public int hashCode() {
            int result = getName() != null ? getName().hashCode() : 0;
            result = 31 * result + (int) (getCurrentLatency() ^ (getCurrentLatency() >>> 32));
            result = 31 * result + (int) (getStartTimestamp() ^ (getStartTimestamp() >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof FaultItem))
                return false;

            final FaultItem faultItem = (FaultItem) o;

            if (getCurrentLatency() != faultItem.getCurrentLatency())
                return false;
            if (getStartTimestamp() != faultItem.getStartTimestamp())
                return false;
            return getName() != null ? getName().equals(faultItem.getName()) : faultItem.getName() == null;

        }

        @Override
        public String toString() {
            return "FaultItem{" +
                "name='" + name + '\'' +
                ", currentLatency=" + currentLatency +
                ", startTimestamp=" + startTimestamp +
                '}';
        }

        public String getName() {
            return name;
        }

        public long getCurrentLatency() {
            return currentLatency;
        }

        public void setCurrentLatency(final long currentLatency) {
            this.currentLatency = currentLatency;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(final long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

    }
}
