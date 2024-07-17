package com.datastax.cdm.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.oss.driver.api.core.ConsistencyLevel;

class CqlTableTest extends CommonMocks {

	@Test
	void testCL() {
		assertEquals(CqlTable.mapToConsistencyLevel("LOCAL_QUORUM"), ConsistencyLevel.LOCAL_QUORUM);
		assertEquals(CqlTable.mapToConsistencyLevel("any"), ConsistencyLevel.ANY);
		assertEquals(CqlTable.mapToConsistencyLevel("one"), ConsistencyLevel.ONE);
		assertEquals(CqlTable.mapToConsistencyLevel("two"), ConsistencyLevel.TWO);
		assertEquals(CqlTable.mapToConsistencyLevel("three"), ConsistencyLevel.THREE);
		assertEquals(CqlTable.mapToConsistencyLevel("QUORUM"), ConsistencyLevel.QUORUM);
		assertEquals(CqlTable.mapToConsistencyLevel("Local_one"), ConsistencyLevel.LOCAL_ONE);
		assertEquals(CqlTable.mapToConsistencyLevel("EACH_quorum"), ConsistencyLevel.EACH_QUORUM);
		assertEquals(CqlTable.mapToConsistencyLevel("serial"), ConsistencyLevel.SERIAL);
		assertEquals(CqlTable.mapToConsistencyLevel("local_serial"), ConsistencyLevel.LOCAL_SERIAL);
		assertEquals(CqlTable.mapToConsistencyLevel("all"), ConsistencyLevel.ALL);
	}

}
