package dev.purpose.distrib_counter.core;

import dev.purpose.distrib_counter.utils.IdempotencyToken;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable value object returned by {@link Counter} operations.
 *
 * <p>Encapsulates:</p>
 *
 * @param value       the observed counter-value at the given timestamp.
 * @param timestamp   the logical instant at which this value was measured or computed.
 * @param consistency the level of consistency associated with the value
 *                    (best-effort, eventually consistent, or accurate).
 * @param token       optional {@link IdempotencyToken} used for deduplication, may be {@code null}.
 *
 * <h4>Immutability:</h4>
 * This class is a {@code record}, ensuring immutability and safe sharing across threads.
 * @author Riyane
 * @version 1.0.0
 * @see Counter
 * @see CounterConsistency
 */
public record CounterResult(
		long value,
		Instant timestamp,
		CounterConsistency consistency,
		IdempotencyToken token
) {
	public CounterResult(long value, Instant timestamp, CounterConsistency consistency, IdempotencyToken token) {
		this.value = value;
		this.timestamp = Objects.requireNonNull(timestamp, "timestamp must be not null");
		this.consistency = Objects.requireNonNull(consistency, "consistency must be not null");
		this.token = token;
	}

	@NotNull
	@Override
	public String toString() {
		return "CounterResult(value=%d, timestamp=%s, consistency=%s, token=%s)".formatted(
				value,
				timestamp,
				consistency,
				token != null ? token.asString() : "null"
		);
	}
}
