package dev.purpose.distrib_counter.impl;

import dev.purpose.distrib_counter.core.AsyncCounter;
import dev.purpose.distrib_counter.core.CounterConsistency;
import dev.purpose.distrib_counter.core.CounterException;
import dev.purpose.distrib_counter.core.CounterResult;
import dev.purpose.distrib_counter.infra.RedisSentinelManager;
import dev.purpose.distrib_counter.utils.CounterUtils;
import dev.purpose.distrib_counter.utils.IdempotencyToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public record BestEffortAsyncCounter(RedisSentinelManager<String, String> manager) implements AsyncCounter {
	private static final Logger log = LoggerFactory.getLogger(BestEffortAsyncCounter.class);

	public BestEffortAsyncCounter(RedisSentinelManager<String, String> manager) {
		this.manager = Objects.requireNonNull(manager, "manager must be not null");
	}

	@Override
	public CompletionStage<Void> add(String namespace, String counterName, long delta, IdempotencyToken token) throws CounterException {
		return Objects.requireNonNull(addAndGet(namespace, counterName, delta, token))
				.thenApply(counterResult -> null);
	}

	@Override
	public CompletionStage<CounterResult> addAndGet(String namespace, String counterName, long delta, IdempotencyToken token) throws CounterException {
		return manager.executeAsync(commands -> {
			String counterKey = CounterUtils.key(namespace, counterName);
			CompletableFuture<CounterResult> future = new CompletableFuture<>();

			if (token != null) {
				String idempotencyKey = CounterUtils.idempotencyKey(namespace, counterName, token);
				commands.exists(idempotencyKey).thenAccept(exists -> {
					if (exists != null && exists > 0) {
						commands.get(counterKey).thenAccept(value -> {
							long current = CounterUtils.parseLong(value);
							future.complete(new CounterResult(current, Instant.now(), CounterConsistency.BEST_EFFORT, token));
						}).exceptionally(throwable -> {
							future.completeExceptionally(throwable);
							return null;
						});
					} else {
						commands.set(idempotencyKey, "1").thenCompose(ok ->
								commands.incrby(counterKey, delta)
						).thenAccept(newValue ->
								future.complete(new CounterResult(newValue, Instant.now(), CounterConsistency.BEST_EFFORT, token))
						).exceptionally(throwable -> {
							future.completeExceptionally(throwable);
							return null;
						});
					}
				}).exceptionally(throwable -> {
					future.completeExceptionally(throwable);
					return null;
				});
			} else {
				commands.incrby(counterKey, delta).thenAccept(newValue -> {
					future.complete(new CounterResult(newValue, Instant.now(), CounterConsistency.BEST_EFFORT, null));
				}).exceptionally(throwable -> {
					future.completeExceptionally(throwable);
					return null;
				});
			}

			return future;
		});
	}

	@Override
	public CompletionStage<Void> get(String namespace, String counterName) throws CounterException {
		return null;
	}

	@Override
	public CompletionStage<Void> clear(String namespace, String counterName, IdempotencyToken token) throws CounterException {
		return null;
	}
}
