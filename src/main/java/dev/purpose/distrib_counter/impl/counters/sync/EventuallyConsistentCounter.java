package dev.purpose.distrib_counter.impl.counters.sync;

import dev.purpose.distrib_counter.core.Counter;
import dev.purpose.distrib_counter.core.CounterConsistency;
import dev.purpose.distrib_counter.core.CounterException;
import dev.purpose.distrib_counter.core.CounterResult;
import dev.purpose.distrib_counter.infra.RedisSentinelManager;
import dev.purpose.distrib_counter.utils.CounterUtils;
import dev.purpose.distrib_counter.utils.IdempotencyToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Eventually-consistent counter based on Redis HASH.
 *
 * <p>Each counter is stored as a Redis hash under a key:
 * <pre>counter:{namespace}:{counterName}</pre>
 * <p>
 * Each node contributes deltas via {@code HINCRBY} on its own field
 * (field name = nodeId). Reading sums all fields via {@code HGETALL}.
 * <p>
 * This design avoids expensive SCAN/KEYS operations and ensures
 * O(number of nodes) complexity per read.</p>
 * <p>
 * Guarantees: {@link CounterConsistency#EVENTUALLY_CONSISTENT}
 *
 * <h4>Consistency:</h4>
 * Returned values are marked as {@link CounterConsistency#EVENTUALLY_CONSISTENT}.
 *
 * <h4>Thread-safety:</h4>
 * This class is safe for concurrent use.
 *
 * @param manager Sentinel manager
 * @param nodeId  Local node Id
 * @author Riyane
 * @version 0.9.5
 */
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
				commands.hincrby(CounterUtils.key(namespace, counterName), nodeId, delta);
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
				String counterKey = CounterUtils.key(namespace, counterName);
				Map<String, String> allDeltas = commands.hgetall(counterKey);
				long total = allDeltas.values()
						.stream()
						.mapToLong(CounterUtils::parseLong)
						.sum();

				return new CounterResult(total, Instant.now(), CounterConsistency.EVENTUALLY_CONSISTENT, null);
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

				String counterKey = CounterUtils.key(namespace, counterName);
				commands.del(counterKey);
				return null;
			});
		} catch (Exception exception) {
			log.error("Redis clear failed", exception);
			throw new CounterException("Failed to clear counter", "REDIS_ERROR", exception);
		}
	}
}
