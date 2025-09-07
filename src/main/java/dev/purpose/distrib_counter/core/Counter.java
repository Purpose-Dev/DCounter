package dev.purpose.distrib_counter.core;

import dev.purpose.distrib_counter.utils.IdempotencyToken;

/**
 * Contract for a distributed counter-abstraction.
 *
 * <p>A {@code Counter} represents a shared, incrementable value
 * maintained across distributed systems. Implementations differ
 * in the guarantees they provide:
 * <ul>
 *     <li>{@link CounterConsistency#BEST_EFFORT}: direct, low-latency, increments with no global ordering guarantee.</li>
 *     <li>{@link CounterConsistency#EVENTUALLY_CONSISTENT}: values converge asynchronously via replication or rollup.</li>
 *     <li>{@link CounterConsistency#ACCURATE}: values reflect a strongly consistent snapshot at the given timestamp.</li>
 * </ul></p>
 *
 * <p>All mutating operations may accept an {@link IdempotencyToken}.
 * When provided, repeated calls with the same token must not produce duplicate
 * increments or clears, ensuring exactly once semantics across retries</p>
 *
 * <h4>Thread-safety:</h4>
 * Implementations must be safe for concurrent use by multiple threads.
 *
 * <h4>Failure handling:</h4>
 * Any infrastructure-level failure (Redis unavailability, broker errors)
 * is reported as a {@link CounterException}.
 *
 * @author Riyane
 * @version 1.0.0
 */
public interface Counter {

	/**
	 * Add a delta to the counter.
	 *
	 * @param namespace   logical namespace
	 * @param counterName counter name
	 * @param delta       amount to add (positive or negative)
	 * @param token       optional idempotency token (maybe null for best-effort)
	 * @throws CounterException if the operation fails
	 */
	void add(String namespace, String counterName, long delta, IdempotencyToken token)
			throws CounterException;

	default void add(String namespace, String counterName, long delta) throws CounterException {
		add(namespace, counterName, delta, null);
	}

	/**
	 * Add a delta to the counter and get it.
	 *
	 * @param namespace   logical namespace
	 * @param counterName counter name
	 * @param delta       amount to add (positive or negative)
	 * @param token       optional idempotency token (maybe null for best-effort)
	 * @return Counter result of operation
	 * @throws CounterException if the operation fails
	 */
	CounterResult addAndGet(String namespace, String counterName, long delta, IdempotencyToken token)
			throws CounterException;

	default CounterResult addAndGet(String namespace, String counterName, long delta) throws CounterException {
		return addAndGet(namespace, counterName, delta, null);
	}

	/**
	 * Get the current value of the counter.
	 *
	 * @param namespace   logical namespace
	 * @param counterName counter name
	 * @return current counter value
	 * @throws CounterException if the operation fails
	 */
	CounterResult get(String namespace, String counterName) throws CounterException;

	/**
	 * Clear (reset) the counter.
	 *
	 * @param namespace   logical namespace
	 * @param counterName counter name
	 * @param token       optional idempotency token (maybe null for best-effort)
	 * @throws CounterException if the operation fails
	 */
	void clear(String namespace, String counterName, IdempotencyToken token) throws CounterException;

	default void clear(String namespace, String counterName) throws CounterException {
		clear(namespace, counterName, null);
	}
}
