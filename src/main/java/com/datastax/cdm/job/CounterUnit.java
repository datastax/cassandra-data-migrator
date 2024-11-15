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

public class CounterUnit implements Serializable {

    private static final long serialVersionUID = 2194336948011681878L;
    private long count = 0;
    private long interimCount = 0;

    public void increment(long incrementBy) {
        interimCount += incrementBy;
    }

    public long getInterimCount() {
        return interimCount;
    }

    public void reset() {
        interimCount = 0;
    }

    public void setCount(long value) {
        count = value;
    }

    public void addToCount() {
        count += interimCount;
        reset();
    }

    public long getCount() {
        return count;
    }
}
