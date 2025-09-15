package dev.purpose.distrib_counter.impl;

import dev.purpose.distrib_counter.core.AsyncCounter;
import dev.purpose.distrib_counter.core.Counter;
import dev.purpose.distrib_counter.infra.RedisSentinelManager;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Factory to create namespace rollup scheduler (sync or async).
 *
 * <p>Chooses automatically the right scheduler implementation
 * depending on the type of counter used (sync or async).</p>
 *
 * @author Riyane
 * @version 1.0.0
 */
public final class RollupSchedulerFactory {

	private RollupSchedulerFactory() {
	}

	/**
	 * Create a rollup scheduler bound to a {@link Counter} (sync).
	 *
	 * @param manager   Redis sentinel manager
	 * @param scheduler Scheduler service
	 * @param interval  Rollup interval
	 * @param counter   Counter instance
	 * @return scheduler instance (sync or async).
	 */
	public static AutoCloseable create(
			RedisSentinelManager<String, String> manager,
			ScheduledExecutorService scheduler,
			Duration interval,
			Object counter
	) {
		Objects.requireNonNull(manager, "manager must not be null");
		Objects.requireNonNull(scheduler, "scheduler must not be null");
		Objects.requireNonNull(interval, "interval must not be null");
		Objects.requireNonNull(counter, "counter must not be null");

		if (counter instanceof AsyncCounter) {
			return new AsyncNamespaceRollupScheduler(manager, scheduler, interval);
		} else if (counter instanceof Counter) {
			return new NamespaceRollupScheduler(manager, scheduler, interval);
		} else {
			final String s = "Unsupported counter type: %s".formatted(counter.getClass().getName());
			throw new IllegalArgumentException(s);
		}
	}
}
