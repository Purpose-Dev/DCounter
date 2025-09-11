package dev.purpose.distrib_counter.impl;

import dev.purpose.distrib_counter.core.AsyncCounter;
import dev.purpose.distrib_counter.core.Counter;
import dev.purpose.distrib_counter.core.CounterConsistency;
import dev.purpose.distrib_counter.infra.RedisSentinelManager;

import java.util.Objects;

/**
 * Centralized factory for creating both synchronous {@link Counter} and asynchronous {@link AsyncCounter}
 * implementations backed by Redis.
 *
 * <p>Selection is based on the requested {@link CounterConsistency} level.</p>
 *
 * <h4>Usage example:</h4>
 * <pre>{@code
 * RedisSentinelManager<String, String> manager = new RedisSentinelManager(config);
 * CounterFactory factory = new CounterFactory(manager);
 *
 * Counter sync = factory.createCounter(CounterConsistency.BEST_EFFORT);
 * AsyncCounter async = factory.createAsyncCounter(CounterConsistency.BEST_EFFORT);
 * }</pre>
 *
 * <h4>Extensibility:</h4>
 * New implementations can be added to the switch logic without impacting clients.
 *
 * @author Riyane
 * @version 1.0.0
 */
public record CounterFactory(RedisSentinelManager<String, String> manager) {
	public CounterFactory(RedisSentinelManager<String, String> manager) {
		this.manager = Objects.requireNonNull(manager, "manager must not be null");
	}

	/**
	 * Exposes the underlying Redis manager for advanced usage.
	 *
	 * @return the underlying Redis manager.
	 */
	public RedisSentinelManager<String, String> manager() {
		return manager;
	}

	/**
	 * Create a synchronous counter for the given consistency level.
	 *
	 * @param consistency desired consistency level
	 * @return a {@link Counter} implementation
	 */
	public Counter createCounter(CounterConsistency consistency) {
		return switch (consistency) {
			case BEST_EFFORT -> new BestEffortCounter(manager);
			/*case EVENTUALLY_CONSISTENT -> null;
			case ACCURATE -> null;*/
			default -> throw new UnsupportedOperationException(
					"Unsupported consistency: " + consistency
			);
		};
	}

	/**
	 * Create an asynchronous counter for the given consistency level.
	 *
	 * @param consistency desired consistency level
	 * @return a {@link AsyncCounter} implementation
	 */
	public AsyncCounter createAsyncCounter(CounterConsistency consistency) {
		return switch (consistency) {
			case BEST_EFFORT -> new BestEffortAsyncCounter(manager);
			/*case EVENTUALLY_CONSISTENT -> null;
			case ACCURATE -> null;*/
			default -> throw new UnsupportedOperationException(
					"Unsupported consistency: " + consistency
			);
		};
	}
}
