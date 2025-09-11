package dev.purpose.distrib_counter.impl;

import dev.purpose.distrib_counter.core.AsyncCounter;
import dev.purpose.distrib_counter.core.Counter;
import dev.purpose.distrib_counter.core.CounterConsistency;
import dev.purpose.distrib_counter.impl.async.BestEffortAsyncCounter;
import dev.purpose.distrib_counter.impl.async.EventuallyConsistentAsyncCounter;
import dev.purpose.distrib_counter.impl.sync.BestEffortCounter;
import dev.purpose.distrib_counter.impl.sync.EventuallyConsistentCounter;
import dev.purpose.distrib_counter.infra.RedisSentinelManager;

import java.util.Objects;

/**
 * Factory for building synchronous or asynchronous counters
 * with a chosen consistency model.
 *
 * <p>Supports:
 * <ul>
 *     <li>{@link CounterConsistency#BEST_EFFORT}</li>
 *     <li>{@link CounterConsistency#EVENTUALLY_CONSISTENT}</li>
 * </ul></p>
 * <p>
 * Usage:
 * <pre>
 * Counter c1 = CounterFactory.createCounter(manager, CounterConsistency.BEST_EFFORT);
 * AsyncCounter c2 = CounterFactory.createAsyncCounter(manager, CounterConsistency.EVENTUALLY_CONSISTENT, "nodeA");
 * </pre>
 *
 * @author Riyane
 * @version 0.9.8
 */
public final class CounterFactory {

	private CounterFactory() {
	}

	/**
	 * Create a synchronous counter with the given consistency.
	 *
	 * @param manager     Redis sentinel manager
	 * @param consistency desired consistency model
	 * @param nodeId      required for eventually consistent counters (ignored otherwise)
	 * @return a Counter implementation
	 */
	public static Counter createCounter(
			RedisSentinelManager<String, String> manager,
			CounterConsistency consistency,
			String nodeId
	) {
		Objects.requireNonNull(manager, "manager must not be null");
		Objects.requireNonNull(consistency, "consistency must not be null");

		return switch (consistency) {
			case BEST_EFFORT -> new BestEffortCounter(manager);
			case EVENTUALLY_CONSISTENT -> {
				Objects.requireNonNull(nodeId, "nodeId required for eventually consistent counter");
				yield new EventuallyConsistentCounter(manager, nodeId);
			}
			case ACCURATE -> throw new UnsupportedOperationException("Accurate counter not yet implemented");
		};
	}

	/**
	 * Create an asynchronous counter with the given consistency.
	 *
	 * @param manager     Redis sentinel manager
	 * @param consistency desired consistency model
	 * @param nodeId      required for eventually consistent counters (ignored otherwise)
	 * @return an AsyncCounter implementation
	 */
	public static AsyncCounter createAsyncCounter(
			RedisSentinelManager<String, String> manager,
			CounterConsistency consistency,
			String nodeId
	) {
		Objects.requireNonNull(manager, "manager must not be null");
		Objects.requireNonNull(consistency, "consistency must not be null");

		return switch (consistency) {
			case BEST_EFFORT -> new BestEffortAsyncCounter(manager);
			case EVENTUALLY_CONSISTENT -> {
				Objects.requireNonNull(nodeId, "nodeId required for eventually consistent async counter");
				yield new EventuallyConsistentAsyncCounter(manager, nodeId);
			}
			case ACCURATE -> throw new UnsupportedOperationException("Accurate async counter not yet implemented");
		};
	}
}
