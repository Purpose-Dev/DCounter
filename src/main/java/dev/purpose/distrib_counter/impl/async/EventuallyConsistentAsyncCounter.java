package dev.purpose.distrib_counter.impl.async;

import dev.purpose.distrib_counter.core.AsyncCounter;
import dev.purpose.distrib_counter.core.CounterConsistency;
import dev.purpose.distrib_counter.core.CounterException;
import dev.purpose.distrib_counter.core.CounterResult;
import dev.purpose.distrib_counter.infra.RedisSentinelManager;
import dev.purpose.distrib_counter.utils.CounterUtils;
import dev.purpose.distrib_counter.utils.IdempotencyToken;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public record EventuallyConsistentAsyncCounter(
		RedisSentinelManager<String, String> manager,
		String nodeId
) implements AsyncCounter {
	@Override
	public CompletionStage<Void> add(String namespace, String counterName, long delta, IdempotencyToken token) throws CounterException {
		return addAndGet(namespace, counterName, delta, token)
				.thenApply(counterResult -> null);
	}

	@Override
	public CompletionStage<CounterResult> addAndGet(String namespace, String counterName, long delta, IdempotencyToken token) throws CounterException {
		String totalKey = CounterUtils.totalKey(namespace, counterName);
		String deltaKey = CounterUtils.deltaKeyForNode(namespace, counterName, nodeId);
		String idempotencyKey = token != null ? CounterUtils.idempotencyKey(namespace, counterName, token) : null;

		return manager.executeAsync(commands -> {
			if (token != null) {
				return commands.exists(idempotencyKey).thenCompose(alreadyExists -> {
					if (alreadyExists > 0) {
						return aggregateValue(commands, totalKey, deltaKey)
								.thenApply(value -> new CounterResult(
										delta, Instant
										.now(),
										CounterConsistency.EVENTUALLY_CONSISTENT,
										token)
								);
					}
					return commands.set(idempotencyKey, "1")
							.thenCompose(x -> commands.hincrby(deltaKey, nodeId, delta))
							.thenCompose(newDelta -> aggregateValue(commands, totalKey, deltaKey))
							.thenApply(value -> new CounterResult(
									delta, Instant
									.now(),
									CounterConsistency.EVENTUALLY_CONSISTENT,
									token)
							);
				});
			} else {
				return commands.hincrby(deltaKey, nodeId, delta)
						.thenCompose(newDelta -> aggregateValue(commands, totalKey, deltaKey))
						.thenApply(value -> new CounterResult(
								delta, Instant
								.now(),
								CounterConsistency.EVENTUALLY_CONSISTENT,
								null)
						);
			}
		});
	}

	@Override
	public CompletionStage<CounterResult> get(String namespace, String counterName) throws CounterException {
		String totalKey = CounterUtils.totalKey(namespace, counterName);
		String deltaKey = CounterUtils.deltaKeyForNode(namespace, counterName, nodeId);

		return manager.executeAsync(commands -> aggregateValue(commands, totalKey, deltaKey)
				.thenApply(value -> new CounterResult(
						value,
						Instant.now(),
						CounterConsistency.EVENTUALLY_CONSISTENT,
						null)
				)
		);
	}

	@Override
	public CompletionStage<Void> clear(String namespace, String counterName, IdempotencyToken token) throws CounterException {
		String totalKey = CounterUtils.totalKey(namespace, counterName);
		String idempotencyKey = token != null ? CounterUtils.idempotencyKey(namespace, counterName, token) : null;

		return manager.executeAsync(commands -> {
			if (token != null) {
				return commands.exists(idempotencyKey).thenCompose(alreadyExists -> {
					if (alreadyExists > 0) {
						return commands.set(totalKey, "0")
								.thenCompose(commands::del)
								.thenApply(x -> null);
					}

					return commands.set(idempotencyKey, "1")
							.thenCompose(x -> commands.set(totalKey, "0"))
							.thenCompose(commands::del)
							.thenApply(x -> null);
				});
			} else {
				return commands.set(totalKey, "0")
						.thenCompose(commands::del)
						.thenApply(x -> null);
			}
		});
	}

	private CompletionStage<Long> aggregateValue(RedisAsyncCommands<String, String> commands, String totalKey, String deltaKey) {
		return commands.get(totalKey).thenCompose(totalStr -> {
			long total = CounterUtils.parseLong(totalStr);
			return commands.hgetall(deltaKey).thenApply(deltas -> {
				long sumDeltas = sumMap(deltas);
				return total + sumDeltas;
			});
		});
	}

	private long sumMap(Map<String, String> map) {
		return map.values()
				.stream()
				.mapToLong(CounterUtils::parseLong)
				.sum();
	}
}
