package dev.purpose.distrib_counter.impl;

import dev.purpose.distrib_counter.infra.RedisSentinelManager;
import dev.purpose.distrib_counter.utils.CounterUtils;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Periodic rollup scheduler that aggregates all counters in a namespace.
 *
 * <p>Scans Redis for all {@code counter:{namespace}:*:deltas} keys, rolls up
 * their values into the corresponding {@code :total} key, and clears deltas.
 * This prevents unbounded growth of delta-hashes and speeds up reads.</p>
 *
 * @author Riyane
 * @version 1.0.0
 */
public final class NamespaceRollupScheduler implements AutoCloseable {
	private static final Logger log = LoggerFactory.getLogger(NamespaceRollupScheduler.class);

	private final RedisSentinelManager<String, String> manager;
	private final ScheduledExecutorService scheduler;
	private final Duration interval;
	private ScheduledFuture<?> future;

	public NamespaceRollupScheduler(
			RedisSentinelManager<String, String> manager,
			ScheduledExecutorService scheduler,
			Duration interval
	) {
		this.manager = Objects.requireNonNull(manager, "manager must be non null");
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler must be non null");
		this.interval = Objects.requireNonNull(interval, "interval must be non null");
	}

	/**
	 * Start periodic rollups for all counters in a namespace.
	 *
	 * @param namespace logical namespace
	 */
	public void start(String namespace) {
		final String pattern = "counter:%s:*:deltas".formatted(namespace);

		this.future = scheduler.scheduleAtFixedRate(() -> {
			try {
				manager.executeSync(commands -> {
					String cursor = "0";
					do {
						var scan = commands.scan(ScanCursor.of(cursor), matchOptions(pattern));
						cursor = scan.getCursor();
						for (String deltaKey : scan.getKeys()) {
							rollupSingle(commands, namespace, deltaKey);
						}
					} while (!"0".equals(cursor));
					return null;
				});
			} catch (Exception exception) {
				log.warn("Namespace rollup failed for {}", namespace);
			}
		}, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);

		log.info("Started rollup scheduler for namespace={} every {}", namespace, interval);
	}

	@Override
	public void close() throws Exception {
		if (future != null) {
			future.cancel(false);
		}
		log.info("NamespaceRollupScheduler stopped");
	}

	private ScanArgs matchOptions(String pattern) {
		return ScanArgs.Builder.matches(pattern).limit(200);
	}

	private void rollupSingle(
			RedisCommands<String, String> commands,
			String namespace,
			String deltaKey
	) {
		Map<String, String> deltas = commands.hgetall(deltaKey);
		if (deltas.isEmpty()) {
			return;
		}

		long sum = deltas.values()
				.stream()
				.mapToLong(CounterUtils::parseLong)
				.sum();

		if (sum != 0) {
			String counterName = extractCounterName(deltaKey);
			String totalKey = CounterUtils.totalKey(namespace, counterName);
			commands.incrby(totalKey, sum);
		}

		commands.del(deltaKey);
		log.debug("Rolled up {} -> deltaKey={}", sum, deltaKey);
	}

	private String extractCounterName(String deltaKey) {
		String[] parts = deltaKey.split(":");
		if (parts.length < 4) {
			return "unknown";
		}

		return parts[2];
	}
}
