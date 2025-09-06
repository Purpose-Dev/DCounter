package dev.purpose.distrib_counter;

import dev.purpose.distrib_counter.security.SecureUUID;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SecureUUIDTest {

	@Test
	void testGenerateNotNull() {
		UUID uuid = SecureUUID.generate();
		assertNotNull(uuid, "Generated UUID must not be null");
	}

	@Test
	void testGenerateAsStringIsValidUUID() {
		String strUuid = SecureUUID.generateAsString();
		assertDoesNotThrow(() -> UUID.fromString(strUuid),
				"Generated string must be parseable as a UUID"
		);
	}

	@RepeatedTest(50)
	void testVersionIsV4() {
		UUID uuid = SecureUUID.generate();
		assertEquals(4, uuid.version(), "UUID version must be 4");
	}

	@RepeatedTest(50)
	void testVariantIsRFC4122() {
		UUID uuid = SecureUUID.generate();
		assertEquals(2, uuid.variant(), "UUID variant must be IETF RFC 4122 (variant 2)");
	}

	@Test
	void testUniqueness() {
		int count = 1_000_000;
		Set<UUID> uuids = new HashSet<>(count);

		for (int i = 0; i < count; i++) {
			UUID uuid = SecureUUID.generate();
			assertTrue(uuids.add(uuid), "Duplicate UUID detected at iteration " + i);
		}
	}
}
