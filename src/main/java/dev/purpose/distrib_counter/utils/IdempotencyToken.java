package dev.purpose.distrib_counter.utils;

import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents an idempotency token used to ensure that
 * requests are not processed more than once in a distributed system.
 *
 * <p>Backed by a cryptographically secure UUID v7 for uniqueness
 * and time-ordering, plus a generation timestamp for traceability.</p>
 *
 * <p>Typical usage:
 * <pre>
 *     IdempotencyToken token = IdempotencyToken.generate();
 *     String tokenValue = token.asString();
 *
 *     // store tokenValue in DB or send in API request
 * </pre></p>
 *
 * @param tokenId        identifier (must be a valid UUID v7 string)
 * @param generationTime timestamp when the token was created
 *
 * @author Riyane
 * @version 1.0.0
 */
public record IdempotencyToken(String tokenId, Instant generationTime) {
	/**
	 * Creates a new token with a given ID and current timestamp.
	 *
	 * @param tokenId identifier (must be a valid UUID v7 string)
	 */
	public IdempotencyToken(String tokenId) {
		this(tokenId, Instant.now());
	}

	/**
	 * Creates a new token with explicit ID and timestamp.
	 *
	 * @param tokenId        identifier (must be a valid UUID v7 string)
	 * @param generationTime timestamp when the token was created
	 */
	public IdempotencyToken {
		Objects.requireNonNull(tokenId, "tokenId must not be null");
		Objects.requireNonNull(generationTime, "generationTime must not be null");

		try {
			UUID.fromString(tokenId);
		} catch (IllegalArgumentException exception) {
			throw new IllegalArgumentException("tokenId must be a valid UUID string", exception);
		}
	}

	/**
	 * Factory method that generates a new cryptographically secure idempotency token.
	 *
	 * @return new IdempotencyToken
	 */
	public static IdempotencyToken generate() {
		return new IdempotencyToken(SecureUUID.generateV7AsString());
	}

	/**
	 * Returns the token ID as string (UUID).
	 *
	 * @return token string
	 */
	@Override
	public String tokenId() {
		return tokenId;
	}

	/**
	 * Returns the token generation timestamp string.
	 *
	 * @return timestamp
	 */
	@Override
	public Instant generationTime() {
		return generationTime;
	}

	/**
	 * Return a serialized representation of this token (for APIs, logs, DB).
	 *
	 * @return token string
	 */
	public String asString() {
		return tokenId;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;

		if (obj instanceof IdempotencyToken(String id, Instant time)) {
			return this.tokenId().equals(id)
					&& this.generationTime().equals(time);
		}

		return false;
	}

	@NotNull
	@Override
	public String toString() {
		return "IdempotencyToken(tokenId=%s, generationTime=%s)"
				.formatted(tokenId, generationTime);
	}
}
