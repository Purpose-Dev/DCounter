package dev.purpose.distrib_counter.impl.async;

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

/**
 * Asynchronous implementation of {@link AsyncCounter} using Redis best-effort semantics.
 *
 * <p>Relies on Redis INCRBY/SET commands without global ordering guarantees.
 * Supports optional idempotency via per-operation Redis keys.</p>
 *
 * @param manager Redis sentinel manager
 * @author Riyane
 * @version 1.0.0
 */
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
	public CompletionStage<CounterResult> get(String namespace, String counterName) throws CounterException {
		return manager.executeAsync(commands -> {
			CompletableFuture<CounterResult> future = new CompletableFuture<>();
			String counterKey = CounterUtils.key(namespace, counterName);

			commands.get(counterKey).thenAccept(val -> {
				long value = CounterUtils.parseLong(val);
				future.complete(new CounterResult(value, Instant.now(), CounterConsistency.BEST_EFFORT, null));
			}).exceptionally(throwable -> {
				future.completeExceptionally(throwable);
				return null;
			});

			return future;
		});
	}

	@Override
	public CompletionStage<Void> clear(String namespace, String counterName, IdempotencyToken token) throws CounterException {
		return manager.executeAsync(commands -> {
			CompletableFuture<Void> future = new CompletableFuture<>();
			String counterKey = CounterUtils.key(namespace, counterName);

			if (token != null) {
				String idempotencyKey = CounterUtils.idempotencyKey(namespace, counterName, token);
				commands.exists(idempotencyKey).thenAccept(exists -> {
					if (exists != null && exists > 0) {
						future.complete(null);
					} else {
						commands.set(idempotencyKey, "1").thenCompose(ok ->
										commands.set(counterKey, "0")
								).thenAccept(ok -> future.complete(null))
								.exceptionally(throwable -> {
									future.completeExceptionally(throwable);
									return null;
								});
					}
				}).exceptionally(throwable -> {
					future.completeExceptionally(throwable);
					return null;
				});
			} else {
				commands.set(counterKey, "0").thenAccept(ok -> future.complete(null))
						.exceptionally(throwable -> {
							future.completeExceptionally(throwable);
							return null;
						});
			}

			return future;
		});
	}
}
