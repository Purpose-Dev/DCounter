package dev.purpose.distrib_counter.core;

import dev.purpose.distrib_counter.utils.IdempotencyToken;

import java.util.concurrent.CompletionStage;

/**
 * Contract for an asynchronous distributed counter-abstraction.
 *
 * <p>Implementations differ in the guarantees they provide:
 * <ul>
 *     <li>{@link CounterConsistency#BEST_EFFORT}: direct, low-latency, increments with no global ordering guarantee.</li>
 *     <li>{@link CounterConsistency#EVENTUALLY_CONSISTENT}: values converge asynchronously via replication or rollup.</li>
 *     <li>{@link CounterConsistency#ACCURATE}: values reflect a strongly consistent snapshot at the given timestamp.</li>
 * </ul></p>
 *
 * <h4>Thread-safety:</h4>
 * Implementations must be safe for concurrent use by multiple threads.
 *
 * <h4>Failure handling:</h4>
 * Any infrastructure-level failure (Redis unavailability, broker errors)
 * is reported vy completing the returned {@code CompletionStage} exceptionally
 * with a {@link CounterException}.
 *
 * @author Riyane
 * @version 1.0.0
 */
public interface AsyncCounter {

	CompletionStage<Void> add(String namespace, String counterName, long delta, IdempotencyToken token) throws CounterException;

	default CompletionStage<Void> add(String namespace, String counterName, long delta) throws CounterException {
		return add(namespace, counterName, delta, null);
	}

	CompletionStage<CounterResult> addAndGet(String namespace, String counterName, long delta, IdempotencyToken token) throws CounterException;

	default CompletionStage<CounterResult> addAndGet(String namespace, String counterName, long delta) throws CounterException {
		return addAndGet(namespace, counterName, delta, null);
	}

	default CompletionStage<Void> decrement(String namespace, String counterName, IdempotencyToken token) throws CounterException {
		return add(namespace, counterName, -1, token);
	}

	default CompletionStage<Void> decrement(String namespace, String counterName) throws CounterException {
		return add(namespace, counterName, -1);
	}

	default CompletionStage<CounterResult> decrementAndGet(String namespace, String counterName, IdempotencyToken token) throws CounterException {
		return addAndGet(namespace, counterName, -1, token);
	}

	default CompletionStage<CounterResult> decrementAndGet(String namespace, String counterName) throws CounterException {
		return addAndGet(namespace, counterName, -1);
	}

	CompletionStage<Void> get(String namespace, String counterName) throws CounterException;

	;

	CompletionStage<Void> clear(String namespace, String counterName, IdempotencyToken token) throws CounterException;

	default CompletionStage<Void> clear(String namespace, String counterName) throws CounterException {
		return clear(namespace, counterName, null);
	}
}
