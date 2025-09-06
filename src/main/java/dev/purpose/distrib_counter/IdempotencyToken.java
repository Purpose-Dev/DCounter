package dev.purpose.distrib_counter;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@SuppressWarnings("ClassCanBeRecord")
public final class IdempotencyToken {
	private final String tokenId;
	private final Instant generationTime;

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
	 * @param tokenId identifier (must be a valid UUID v7 string)
	 * @param generationTime timestamp when the token was created
	 */
	public IdempotencyToken(String tokenId, Instant generationTime) {
		Objects.requireNonNull(tokenId, "tokenId must not be null");
		Objects.requireNonNull(generationTime, "generationTime must not be null");

		try {
			UUID.fromString(tokenId);
		} catch (IllegalArgumentException exception) {
			throw new IllegalArgumentException("tokenId must be a valid UUID string", exception);
		}

		this.tokenId = tokenId;
		this.generationTime = generationTime;
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
	public String getTokenId() {
		return tokenId;
	}

	/**
	 * Returns the token generation timestamp string.
	 *
	 * @return timestamp
	 */
	public Instant getGenerationTime() {
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
	public int hashCode() {
		return Objects.hash(tokenId, generationTime);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;

		if (obj instanceof IdempotencyToken token) {
			return this.getTokenId().equals(token.getTokenId())
					&& this.getGenerationTime().equals(token.getGenerationTime());
		}

		return super.equals(obj);
	}

	@Override
	public String toString() {
		return "IdempotencyToken(tokenId=%s, generationTime=%s)"
				.formatted(tokenId, generationTime);
	}
}
