package dev.purpose.distrib_counter.impl.sync;

import dev.purpose.distrib_counter.core.Counter;
import dev.purpose.distrib_counter.core.CounterConsistency;
import dev.purpose.distrib_counter.core.CounterException;
import dev.purpose.distrib_counter.core.CounterResult;
import dev.purpose.distrib_counter.infra.RedisSentinelManager;
import dev.purpose.distrib_counter.utils.CounterUtils;
import dev.purpose.distrib_counter.utils.IdempotencyToken;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public record EventuallyConsistentCounter(
		RedisSentinelManager<String, String> manager,
		String nodeId
) implements Counter {
	private static final Logger log = LoggerFactory.getLogger(EventuallyConsistentCounter.class);

	public EventuallyConsistentCounter(RedisSentinelManager<String, String> manager, String nodeId) {
		this.manager = Objects.requireNonNull(manager, "manager must not be null");
		this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
	}

	@Override
	public void add(String namespace, String counterName, long delta, IdempotencyToken token) throws CounterException {
		try {
			manager.executeSync(commands -> {
				if (token != null) {
					String idempotencyKey = CounterUtils.idempotencyKey(namespace, counterName, token);
					if (commands.exists(idempotencyKey) > 0) {
						return null;
					}
					commands.set(idempotencyKey, "1");
				}
				commands.incrby(CounterUtils.deltaKeyForNode(namespace, counterName, nodeId), delta);
				return null;
			});
		} catch (Exception exception) {
			log.error("Redis add failed", exception);
			throw new CounterException("Failed to add delta", "REDIS_ERROR", exception);
		}
	}

	@Override
	public CounterResult addAndGet(String namespace, String counterName, long delta, IdempotencyToken token) throws CounterException {
		add(namespace, counterName, delta, token);
		return get(namespace, counterName);
	}

	@Override
	public CounterResult get(String namespace, String counterName) throws CounterException {
		try {
			return manager.executeSync(commands -> {
				String totalKey = CounterUtils.totalKey(namespace, counterName);
				long total = CounterUtils.parseLong(commands.get(totalKey));

				String pattern = CounterUtils.deltaKeyPattern(namespace, counterNamePrefix() + "*");
				long deltaSum = sumDeltasViaScan(commands, pattern);

				long value = total + deltaSum;
				return new CounterResult(value, Instant.now(), CounterConsistency.EVENTUALLY_CONSISTENT, null);
			});
		} catch (Exception exception) {
			log.error("Redis get failed", exception);
			throw new CounterException("Failed to read counter", "REDIS_ERROR", exception);
		}
	}

	@Override
	public void clear(String namespace, String counterName, IdempotencyToken token) throws CounterException {
		try {
			manager.executeSync(commands -> {
				if (token != null) {
					String idempotencyKey = CounterUtils.idempotencyKey(namespace, counterName, token);
					if (commands.exists(idempotencyKey) > 0) {
						return null;
					}
					commands.set(idempotencyKey, "1");
				}

				String totalKey = CounterUtils.totalKey(namespace, counterName);
				commands.set(totalKey, "0");

				String pattern = CounterUtils.deltaKeyPattern(namespace, counterNamePrefix() + "*");
				deleteKeysViaScan(commands, pattern);
				return null;
			});
		} catch (Exception exception) {
			log.error("Redis clear failed", exception);
			throw new CounterException("Failed to clear counter", "REDIS_ERROR", exception);
		}
	}

	private String counterNamePrefix() {
		return ""; // for key scanning convenience
	}

	private long sumDeltasViaScan(RedisCommands<String, String> commands, String pattern) {
		long sum = 0L;
		ScanArgs scanArgs = ScanArgs.Builder.matches(pattern).limit(200);
		KeyScanCursor<String> cursor = commands.scan(scanArgs);

		for (String key : cursor.getKeys()) {
			sum += CounterUtils.parseLong(commands.get(key));
		}

		while (!cursor.isFinished()) {
			cursor = commands.scan(cursor);
			for (String key : cursor.getKeys()) {
				sum += CounterUtils.parseLong(commands.get(key));
			}
		}

		return sum;
	}

	private void deleteKeysViaScan(RedisCommands<String, String> commands, String pattern) {
		ScanArgs scanArgs = ScanArgs.Builder.matches(pattern).limit(200);
		KeyScanCursor<String> cursor = commands.scan(scanArgs);

		for (String key : cursor.getKeys()) {
			commands.del(key);
		}

		while (!cursor.isFinished()) {
			cursor = commands.scan(cursor);
			for (String key : cursor.getKeys()) {
				commands.del(key);
			}
		}
	}
}
