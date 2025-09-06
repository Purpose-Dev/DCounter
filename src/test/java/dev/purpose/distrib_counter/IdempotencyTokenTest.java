package dev.purpose.distrib_counter;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class IdempotencyTokenTest {

	@Test
	void testGenerateNonNull() {
		IdempotencyToken token = IdempotencyToken.generate();
		assertNotNull(token, "Generated token must not be null");
		assertNotNull(token.tokenId(), "Token ID must not be null");
		assertNotNull(token.generationTime(), "Generation time must not be null");
	}

	@Test
	void testGeneratedTokenIsValidUUIDV7() {
		IdempotencyToken token = IdempotencyToken.generate();
		assertDoesNotThrow(() -> UUID.fromString(token.tokenId()),
				"Generated token ID must be a valid UUID string"
		);
		assertEquals(7, UUID.fromString(token.tokenId()).version(),
				"Generation time must not be after the test end");
	}

	@RepeatedTest(50)
	void testGeneratedTokenHasCurrentTimestamp() {
		Instant before = Instant.now();
		IdempotencyToken token = IdempotencyToken.generate();
		Instant after = Instant.now();

		assertFalse(token.generationTime().isBefore(before),
				"Generation time must not be before the test start");
		assertFalse(token.generationTime().isAfter(after),
				"Generation time must not be after the test end");
	}

	void testUniquenessOfGeneratedTokens() {
		int count = 1_000_000;
		Set<String> ids = new HashSet<>(count);

		for (int i = 0; i < count; i++) {
			IdempotencyToken token = IdempotencyToken.generate();
			assertTrue(ids.add(token.tokenId()),
					"Duplicate token detected at iteration " + i);
		}
	}

	@Test
	void testConstructorRejectsNullTokenId() {
		assertThrows(NullPointerException.class,
				() -> new IdempotencyToken(null),
				"Constructor must reject null tokenId");
	}

	@Test
	void testConstructorRejectsNullTimestamp() {
		assertThrows(NullPointerException.class,
				() -> new IdempotencyToken(SecureUUID.generateV7AsString(), null),
				"Constructor must reject invalid UUID string");
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"",
			"not-a-uuid",
			"123456",
			"zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz",
			"00000000-0000-0000-0000-0000000000000"
	})
	void testConstructorRejectsInvalidUUIDString(String invalidUuid) {
		assertThrows(IllegalArgumentException.class,
				() -> new IdempotencyToken(invalidUuid),
				"Constructor must reject invalid UUID string");
	}

	@Test
	void testAsStringReturnsTokenId() {
		IdempotencyToken token = IdempotencyToken.generate();
		assertEquals(token.tokenId(), token.asString(),
				"asString() must return the tokenId");
	}

	@Test
	void testEqualsAndHashCode() {
		String uuid = SecureUUID.generateV7AsString();
		Instant now = Instant.now();

		IdempotencyToken token1 = new IdempotencyToken(uuid, now);
		IdempotencyToken token2 = new IdempotencyToken(uuid, now);

		assertEquals(token1, token2, "Tokens with same id and timestamp must be equal");
		assertEquals(token1.hashCode(), token2.hashCode(),
				"Equal tokens must have equal hashCode");
	}

	@Test
	void testNotEqualsWithDifferentId() {
		Instant now = Instant.now();

		IdempotencyToken token1 = new IdempotencyToken(SecureUUID.generateV7AsString(), now);
		IdempotencyToken token2 = new IdempotencyToken(SecureUUID.generateV7AsString(), now);

		assertNotEquals(token1, token2, "Tokens with different IDs must not be equal");
	}

	@Test
	void testNotEqualsWithDifferentTimestamp() {
		String uuid = SecureUUID.generateV7AsString();

		IdempotencyToken token1 = new IdempotencyToken(uuid, Instant.now());
		IdempotencyToken token2 = new IdempotencyToken(uuid, Instant.now().plusSeconds(1));

		assertNotEquals(token1, token2, "Tokens with different timestamps must not be equal");
	}

	@Test
	void testToStringFormat() {
		IdempotencyToken token = IdempotencyToken.generate();
		String string = token.toString();

		assertTrue(string.contains("IdempotencyToken"),
				"toString() must contain class name");
		assertTrue(string.contains(token.tokenId()),
				"toString() must contain tokenId");
		assertTrue(string.contains(token.generationTime().toString()),
				"toString() must contain generationTime");
	}
}
