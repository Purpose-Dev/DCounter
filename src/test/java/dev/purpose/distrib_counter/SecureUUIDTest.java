package dev.purpose.distrib_counter;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SecureUUIDTest {

	@Test
	void testGenerateNotNullV4() {
		UUID uuid = SecureUUID.generateV4();
		assertNotNull(uuid, "Generated UUID must not be null");
	}

	@Test
	void testGenerateAsStringIsValidUUIDV4() {
		String strUuid = SecureUUID.generateV4AsString();
		assertDoesNotThrow(() -> UUID.fromString(strUuid),
				"Generated string must be parseable as a UUID"
		);
	}

	@RepeatedTest(50)
	void testVersionIsV4() {
		UUID uuid = SecureUUID.generateV4();
		assertEquals(4, uuid.version(), "UUID version must be 4");
	}

	@RepeatedTest(50)
	void testV4VariantIsRFC4122() {
		UUID uuid = SecureUUID.generateV4();
		assertEquals(2, uuid.variant(), "UUID variant must be IETF RFC 4122 (variant 2)");
	}

	@Test
	void testV4Uniqueness() {
		int count = 1_000_000;
		Set<UUID> uuids = new HashSet<>(count);

		for (int i = 0; i < count; i++) {
			UUID uuid = SecureUUID.generateV4();
			assertTrue(uuids.add(uuid), "Duplicate UUID detected at iteration " + i);
		}
	}

	@Test
	void testGenerateNotNullV7() {
		UUID uuid = SecureUUID.generateV7();
		assertNotNull(uuid, "Generated UUID must be not null");
	}

	@Test
	void testGenerateAsStringIsValidUUIDV7() {
		String strUuid = SecureUUID.generateV7AsString();
		assertDoesNotThrow(() -> UUID.fromString(strUuid),
				"Generated string must be parseable as a UUID"
		);
	}

	@RepeatedTest(50)
	void testVersionIsV7() {
		UUID uuid = SecureUUID.generateV7();
		assertEquals(7, uuid.version(), "UUID version must be 4");
	}

	@RepeatedTest(50)
	void testV7VariantIsRFC4122() {
		UUID uuid = SecureUUID.generateV7();
		assertEquals(2, uuid.variant(), "UUID variant must be IETF RFC 4122 (variant 2)");
	}

	@Test
	void testV7Monotonicity() {
		UUID prev = SecureUUID.generateV7();
		for (int i = 0; i < 10_000; i++) {
			UUID next = SecureUUID.generateV7();
			assertTrue(compareTimestamp(prev, next) <= 0,
					"UUID v7 timestamps must be non-decreasing");
			prev = next;
		}
	}

	@Test
	void testV7Uniqueness() {
		int count = 1_000_000;
		Set<UUID> uuids = new HashSet<>(count);

		for (int i = 0; i < count; i++) {
			UUID uuid = SecureUUID.generateV7();
			assertTrue(uuids.add(uuid), "Duplicate UUID detected at iteration " + i);
		}
	}

	private static int compareTimestamp(UUID uuid1, UUID uuid2) {
		long t1 = ((uuid1.getMostSignificantBits()) >>> 16) & 0xFFFFFFFFFFFFL;
		long t2 = ((uuid2.getMostSignificantBits()) >>> 16) & 0xFFFFFFFFFFFFL;
		return Long.compare(t1, t2);
	}
}
