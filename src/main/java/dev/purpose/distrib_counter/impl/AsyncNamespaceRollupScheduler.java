package dev.purpose.distrib_counter.impl;

import dev.purpose.distrib_counter.infra.RedisSentinelManager;
import dev.purpose.distrib_counter.utils.CounterUtils;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Asynchronous rollup scheduler that aggregates all counters in a namespace.
 *
 * <p>Uses {@link RedisAsyncCommands} to scan for all {@code counter:{namespace}:*:deltas}
 * keys, roll up their values into the corresponding {@code :total} key, and clear deltas.</p>
 *
 * @author Riyane
 * @version 1.0.0
 */
public final class AsyncNamespaceRollupScheduler implements AutoCloseable {
	private static final Logger log = LoggerFactory.getLogger(AsyncNamespaceRollupScheduler.class);

	private final RedisSentinelManager<String, String> manager;
	private final ScheduledExecutorService scheduler;
	private final Duration interval;
	private ScheduledFuture<?> future;

	public AsyncNamespaceRollupScheduler(
			RedisSentinelManager<String, String> manager,
			ScheduledExecutorService scheduler,
			Duration interval
	) {
		this.manager = Objects.requireNonNull(manager, "manager must not be null");
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
		this.interval = Objects.requireNonNull(interval, "interval must not be null");
	}

	/**
	 * Start periodic asynchronous rollups for all counters in a namespace.
	 *
	 * @param namespace logical namespace
	 */
	public void start(String namespace) {
		final String pattern = "counter:%s:*:deltas".formatted(namespace);

		this.future = scheduler.scheduleAtFixedRate(() -> {
			try {
				rollupNamespace(namespace, pattern)
						.exceptionally(e -> {
							log.warn("Async namespace rollup failed for {}", namespace, e);
							return null;
						});
			} catch (Exception e) {
				log.error("Unexpected scheduler error", e);
			}
		}, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);

		log.info("Started async rollup scheduler for namespace={} every {}", namespace, interval);
	}

	private CompletionStage<Void> rollupNamespace(String namespace, String pattern) {
		return manager.executeAsync(commands -> scanAndRollup(commands, namespace, pattern));
	}

	private CompletionStage<Void> scanAndRollup(
			RedisAsyncCommands<String, String> commands,
			String namespace,
			String pattern
	) {
		AtomicReference<ScanCursor> cursor = new AtomicReference<>(ScanCursor.INITIAL);
		CompletableFuture<Void> overall = new CompletableFuture<>();

		scanLoop(commands, namespace, pattern, cursor, overall);
		return overall;
	}

	private void scanLoop(
			RedisAsyncCommands<String, String> commands,
			String namespace,
			String pattern,
			AtomicReference<ScanCursor> cursor,
			CompletableFuture<Void> overall
	) {
		commands.scan(cursor.get(), ScanArgs.Builder.matches(pattern).limit(100))
				.whenComplete((scanResult, ex) -> {
					if (ex != null) {
						overall.completeExceptionally(ex);
						return;
					}

					cursor.set(scanResult);
					var futures = scanResult.getKeys().stream()
							.map(deltaKey -> rollupSingle(commands, namespace, deltaKey))
							.toList();

					CompletableFuture
							.allOf(futures.toArray(new CompletableFuture[0]))
							.whenComplete((ignore, err) -> {
								if (err != null) {
									overall.completeExceptionally(err);
									return;
								}
								if (scanResult.isFinished()) {
									overall.complete(null);
								} else {
									scanLoop(commands, namespace, pattern, cursor, overall);
								}
							});
				});
	}

	private CompletableFuture<Void> rollupSingle(
			RedisAsyncCommands<String, String> commands,
			String namespace,
			String deltaKey
	) {
		return commands.hgetall(deltaKey).toCompletableFuture()
				.thenCompose(deltas -> {
					if (deltas.isEmpty()) {
						return CompletableFuture.completedFuture(null);
					}

					long sum = deltas.values().stream()
							.mapToLong(CounterUtils::parseLong)
							.sum();

					if (sum != 0) {
						String counterName = extractCounterName(deltaKey);
						String totalKey = CounterUtils.totalKey(namespace, counterName);
						return commands.incrby(totalKey, sum).toCompletableFuture()
								.thenCompose(ignore -> commands.del(deltaKey).toCompletableFuture())
								.thenAccept(ignore -> log.debug("Rolled up {} → deltaKey={}", sum, deltaKey));
					} else {
						return commands.del(deltaKey).toCompletableFuture()
								.thenAccept(ignore -> log.debug("Deleted empty deltaKey={}", deltaKey));
					}
				});
	}

	private static String extractCounterName(String deltaKey) {
		String[] parts = deltaKey.split(":");
		if (parts.length < 4) {
			return "unknown";
		}
		return parts[2];
	}

	@Override
	public void close() {
		if (future != null) {
			future.cancel(false);
		}
		log.info("AsyncNamespaceRollupScheduler stopped");
	}
}

