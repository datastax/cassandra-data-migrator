/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cdm.job;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class CounterUnit implements Serializable {

    private static final long serialVersionUID = 2194336948011681878L;
    private final AtomicLong globalCounter = new AtomicLong(0);
    private final transient ThreadLocal<Long> threadLocalCounter = ThreadLocal.withInitial(() -> 0L);

    public void incrementThreadCounter(long incrementBy) {
        threadLocalCounter.set(threadLocalCounter.get() + incrementBy);
    }

    public long getThreadCounter() {
        return threadLocalCounter.get();
    }

    public void resetThreadCounter() {
        threadLocalCounter.set(0L);
    }

    public void setGlobalCounter(long value) {
        globalCounter.set(value);
    }

    public void addThreadToGlobalCounter() {
        globalCounter.addAndGet(threadLocalCounter.get());
    }

    public long getGlobalCounter() {
        return globalCounter.get();
    }
}
