package dev.purpose.distrib_counter.impl;

import dev.purpose.distrib_counter.core.AsyncCounter;
import dev.purpose.distrib_counter.core.Counter;
import dev.purpose.distrib_counter.infra.RedisSentinelManager;

import java.util.Objects;

/**
 * Factory for creating {@link Counter} and {@link AsyncCounter}
 * instances.
 *
 * <p>Encapsulates wiring of Redis-backed</p>
 */
public record CounterFactory(RedisSentinelManager<String, String> manager) {
	public CounterFactory(RedisSentinelManager<String, String> manager) {
		this.manager = Objects.requireNonNull(manager, "manager must not be null");
	}

	public Counter bestEffortCounter() {
		return new BestEffortCounter(manager);
	}

	public AsyncCounter bestEffortAsyncCounter() {
		return new BestEffortAsyncCounter(manager);
	}
}
