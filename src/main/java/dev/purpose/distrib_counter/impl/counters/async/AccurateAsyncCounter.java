package dev.purpose.distrib_counter.impl.counters.async;

import dev.purpose.distrib_counter.core.AsyncCounter;
import dev.purpose.distrib_counter.core.CounterConsistency;
import dev.purpose.distrib_counter.core.CounterException;
import dev.purpose.distrib_counter.core.CounterResult;
import dev.purpose.distrib_counter.impl.counters.sync.AccurateCounter;
import dev.purpose.distrib_counter.infra.RedisSentinelManager;
import dev.purpose.distrib_counter.utils.CounterUtils;
import dev.purpose.distrib_counter.utils.IdempotencyToken;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * AccurateAsyncCounter implementation with strong consistency guarantees.
 *
 * <p>Asynchronous variant of {@link AccurateCounter}.</p>
 *
 * <p>Maintains:
 * <ul>
 *     <li>A snapshot key holding the last consolidated value</li>
 *     <li>A deltas hash (HINCRBY per nodeId) holding pending increments</li>
 *     <li>A lastSnapshotTs timestamp for tracking reconciliation freshness</li>
 * </ul>
 *
 * <p>Each read or write triggers reconciliation to ensure exact accuracy.</p>
 */
public record AccurateAsyncCounter(
		RedisSentinelManager<String, String> manager,
		String nodeId
) implements AsyncCounter {
	private static final Logger log = LoggerFactory.getLogger(AccurateAsyncCounter.class);

	public AccurateAsyncCounter(RedisSentinelManager<String, String> manager, String nodeId) {
		this.manager = Objects.requireNonNull(manager, "manager must not be null");
		this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
	}


	@Override
	public CompletionStage<Void> add(String namespace, String counterName, long delta, IdempotencyToken token) throws CounterException {
		return addAndGet(namespace, counterName, delta, token).thenApply(r -> null);
	}

	@Override
	public CompletionStage<CounterResult> addAndGet(String namespace, String counterName, long delta, IdempotencyToken token) {
		return manager.executeAsync(commands -> {
			String snapshotKey = CounterUtils.snapshotKey(namespace, counterName);
			String deltasKey = CounterUtils.deltasKey(namespace, counterName);
			CompletableFuture<CounterResult> future = new CompletableFuture<>();
			CompletionStage<Boolean> tokenCheck;

			if (token != null) {
				String idempotencyKey = CounterUtils.idempotencyKey(namespace, counterName, token);
				tokenCheck = commands.exists(idempotencyKey)
						.thenCompose(exists -> {
							if (exists > 0) {
								return reconcile(commands, snapshotKey, deltasKey)
										.thenApply(v -> {
											future.complete(new CounterResult(v, Instant.now(), CounterConsistency.ACCURATE, token));
											return true;
										});
							} else {
								return commands.set(idempotencyKey, "1").thenApply(ok -> false);
							}
						});
			} else {
				tokenCheck = CompletableFuture.completedFuture(false);
			}

			tokenCheck.whenComplete((alreadyProcessed, error) -> {
				if (error != null) {
					future.completeExceptionally(error);
				} else if (!alreadyProcessed) {
					commands.hincrby(deltasKey, nodeId, delta).whenComplete((v, e) -> {
						if (e != null) {
							future.completeExceptionally(e);
						} else {
							reconcile(commands, snapshotKey, deltasKey)
									.whenComplete((reconciled, err) -> {
										if (err != null) {
											future.completeExceptionally(err);
										} else {
											future.complete(new CounterResult(reconciled, Instant.now(), CounterConsistency.ACCURATE, token));
										}
									});
						}
					});
				}
			});

			return future;
		});
	}

	@Override
	public CompletionStage<CounterResult> get(String namespace, String counterName) throws CounterException {
		return manager.executeAsync(commands -> {
			String snapshotKey = CounterUtils.snapshotKey(namespace, counterName);
			String deltasKey = CounterUtils.deltasKey(namespace, counterName);

			CompletableFuture<CounterResult> future = new CompletableFuture<>();
			reconcile(commands, snapshotKey, deltasKey)
					.whenComplete((reconciled, err) -> {
						if (err != null) {
							future.completeExceptionally(err);
						} else {
							future.complete(new CounterResult(reconciled, Instant.now(), CounterConsistency.ACCURATE, null));
						}
					});

			return future;
		});
	}

	@Override
	public CompletionStage<Void> clear(String namespace, String counterName, IdempotencyToken token) throws CounterException {
		return manager.executeAsync(commands -> {
			String snapshotKey = CounterUtils.snapshotKey(namespace, counterName);
			String deltasKey = CounterUtils.deltasKey(namespace, counterName);
			CompletableFuture<Void> future = new CompletableFuture<>();
			CompletionStage<Boolean> tokenCheck;

			if (token != null) {
				String idempotencyKey = CounterUtils.idempotencyKey(namespace, counterName, token);

				tokenCheck = commands.exists(idempotencyKey)
						.thenCompose(exists -> {
							if (exists > 0) {
								future.complete(null);
								return CompletableFuture.completedFuture(true);
							} else {
								return commands.set(idempotencyKey, "1").thenApply(ok -> false);
							}
						});
			} else {
				tokenCheck = CompletableFuture.completedFuture(false);
			}

			tokenCheck.whenComplete((alreadyProcessed, error) -> {
				if (error != null) {
					future.completeExceptionally(error);
				} else if (!alreadyProcessed) {
					commands.set(snapshotKey, "0").whenComplete((ok, e1) -> {
						if (e1 != null) {
							future.completeExceptionally(e1);
						} else {
							commands.del(deltasKey).whenComplete((ignored, e2) -> {
								if (e2 != null) {
									future.completeExceptionally(e2);
								} else {
									future.complete(null);
								}
							});
						}
					});
				}
			});

			return future;
		});
	}

	private CompletionStage<Long> reconcile(
			RedisAsyncCommands<String, String> commands,
			String snapshotKey,
			String deltasKey
	) {
		CompletableFuture<Long> result = new CompletableFuture<>();

		commands.get(snapshotKey).whenComplete((snapshotVal, e1) -> {
			if (e1 != null) {
				result.completeExceptionally(e1);
				return;
			}

			long snapshot = CounterUtils.parseLong(snapshotVal);

			commands.hgetall(deltasKey).whenComplete((deltas, e2) -> {
				if (e2 != null) {
					result.completeExceptionally(e2);
					return;
				}

				long deltaSum = deltas.values().stream()
						.mapToLong(CounterUtils::parseLong)
						.sum();

				if (deltaSum != 0) {
					long newSnapshot = snapshot + deltaSum;
					commands.set(snapshotKey, Long.toString(newSnapshot)).whenComplete((ok, e3) -> {
						if (e3 != null) {
							result.completeExceptionally(e3);
						} else {
							commands.del(deltasKey).whenComplete((ignored, e4) -> {
								if (e4 != null) {
									result.completeExceptionally(e4);
								} else {
									commands.set(snapshotKey + ":lastSnapshotTs",
													Long.toString(Instant.now().toEpochMilli()))
											.whenComplete((__, e5) -> {
												if (e5 != null) {
													result.completeExceptionally(e5);
												} else {
													result.complete(newSnapshot);
												}
											});
								}
							});
						}
					});
				} else {
					result.complete(snapshot);
				}
			});
		});

		return result;
	}
}
