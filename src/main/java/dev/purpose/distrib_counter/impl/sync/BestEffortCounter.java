package dev.purpose.distrib_counter.impl.sync;

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
import java.util.Objects;

/**
 * Redis-backed implementation of {@link Counter} providing
 * {@link CounterConsistency#BEST_EFFORT} semantics.
 *
 * <p>This counter directly delegates operations to Redis using
 * {@link RedisSentinelManager}, relying on {@code INCRBY}, {@code GET},
 * and {@code SET} commands. It does not guarantee global ordering
 * or strong consistency but offers low latency and high throughput.</p>
 *
 * <h3>Idempotency</h3>
 * <ul>
 *     <li>If an {@link IdempotencyToken} is provided, the counter
 *     ensures that repeated calls with the same token are applied
 *     exactly once.</li>
 *     <li>Idempotency is enforced by creating auxiliary Redis keys
 *     under the form {@code idempotency:{namespace}:{counter}:{token}}</li>
 * </ul>
 *
 * <h3>Thread-safety</h3>
 * This class is fully thread-safe. The underlying
 * {@link RedisSentinelManager} handles connection pooling and retries.
 *
 * <h3>Failure handling</h3>
 * Any Redis-level or infrastructure issue is wrapped
 * in a {@link CounterException} with code {@code REDIS_ERROR}.
 *
 * @author Riyane
 * @version 1.0.0
 */
public record BestEffortCounter(RedisSentinelManager<String, String> manager) implements Counter {
	private static final Logger log = LoggerFactory.getLogger(BestEffortCounter.class);

	/**
	 * Creates a new best-effort counter using Redis.
	 *
	 * @param manager Redis Sentinel manager (must not be {@code null})
	 */
	public BestEffortCounter(RedisSentinelManager<String, String> manager) {
		this.manager = Objects.requireNonNull(manager, "manager must not be null");
	}

	@Override
	public void add(String namespace, String counterName, long delta, IdempotencyToken token) throws CounterException {
		addAndGet(namespace, counterName, delta, token);
	}

	@Override
	public CounterResult addAndGet(String namespace, String counterName, long delta, IdempotencyToken token) throws CounterException {
		try {
			return manager.executeSync(commands -> {
				String counterKey = CounterUtils.key(namespace, counterName);

				if (token != null) {
					String idempotencyKey = CounterUtils.idempotencyKey(namespace, counterName, token);
					boolean already = commands.exists(idempotencyKey) > 0;
					if (already) {
						long current = CounterUtils.parseLong(commands.get(counterKey));
						return new CounterResult(current, Instant.now(), CounterConsistency.BEST_EFFORT, token);
					}
					commands.set(idempotencyKey, "1");
				}

				long newValue = commands.incrby(counterKey, delta);
				return new CounterResult(newValue, Instant.now(), CounterConsistency.BEST_EFFORT, token);
			});
		} catch (Exception exception) {
			log.error("Redis addAndGet failed", exception);
			throw new CounterException("Failed to addAndGet in Redis", "REDIS_ERROR", exception);
		}
	}

	@Override
	public CounterResult get(String namespace, String counterName) throws CounterException {
		try {
			return manager.executeSync(commands -> {
				String counterKey = CounterUtils.key(namespace, counterName);
				long value = CounterUtils.parseLong(commands.get(counterKey));
				return new CounterResult(value, Instant.now(), CounterConsistency.BEST_EFFORT, null);
			});
		} catch (Exception exception) {
			log.error("Redis get failed", exception);
			throw new CounterException("Failed to get counter in Redis", "REDIS_ERROR", exception);
		}
	}

	@Override
	public void clear(String namespace, String counterName, IdempotencyToken token) throws CounterException {
		try {
			manager.executeSync(commands -> {
				String counterKey = CounterUtils.key(namespace, counterName);

				if (token != null) {
					String idempotencyKey = CounterUtils.idempotencyKey(namespace, counterName, token);
					boolean already = commands.exists(idempotencyKey) > 0;
					if (already)
						return null;
					commands.set(idempotencyKey, "1");
				}

				commands.set(counterKey, "0");
				return null;
			});
		} catch (Exception exception) {
			log.error("Redis addAndGet failed", exception);
			throw new CounterException("Failed to clear counter in Redis", "REDIS_ERROR", exception);
		}
	}
}
