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
package com.datastax.cdm.feature;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TrackRunTest {

	@Test
	void test() {
		assertEquals("MIGRATE", TrackRun.RUN_TYPE.MIGRATE.name());
		assertEquals("DIFF_DATA", TrackRun.RUN_TYPE.DIFF_DATA.name());

		assertEquals(2, TrackRun.RUN_TYPE.values().length);
		assertEquals(5, TrackRun.RUN_STATUS.values().length);
	}

}
