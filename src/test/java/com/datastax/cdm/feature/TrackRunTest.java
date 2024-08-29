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
