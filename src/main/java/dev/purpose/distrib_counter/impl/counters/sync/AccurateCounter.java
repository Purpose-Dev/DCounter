package dev.purpose.distrib_counter.impl.counters.sync;

import dev.purpose.distrib_counter.core.Counter;
import dev.purpose.distrib_counter.core.CounterConsistency;
import dev.purpose.distrib_counter.core.CounterException;
import dev.purpose.distrib_counter.core.CounterResult;
import dev.purpose.distrib_counter.infra.RedisSentinelManager;
import dev.purpose.distrib_counter.utils.CounterUtils;
import dev.purpose.distrib_counter.utils.IdempotencyToken;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * AccurateCounter implementation with strong consistency guarantees.
 *
 * <p>This counter maintains:
 * <ul>
 *     <li>A <b>snapshot</b> key holding the last consolidated value</li>
 *     <li>A <b>deltas hash</b> (HINCRBY per nodeId) holding pending increments</li>
 *     <li>A <b>lastSnapshotTs</b> timestamp for tracking reconciliation freshness</li>
 * </ul>
 *
 * <p>Every read operation triggers a full reconciliation: snapshot plus all deltas
 * are aggregated and flushed into the snapshot, ensuring exact accuracy.</p>
 *
 * <p>Idempotency tokens ensure exactly once semantics for increments/clears.</p>
 * <p>
 * Consistency: {@link CounterConsistency#ACCURATE}
 *
 * @author Riyane
 * @version 1.0.0
 */
public record AccurateCounter(RedisSentinelManager<String, String> manager, String nodeId) implements Counter {
	private static final Logger log = LoggerFactory.getLogger(AccurateCounter.class);

	public AccurateCounter(RedisSentinelManager<String, String> manager, String nodeId) {
		this.manager = Objects.requireNonNull(manager, "manager must not be null");
		this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
	}

	@Override
	public void add(String namespace, String counterName, long delta, IdempotencyToken token) throws CounterException {
		addAndGet(namespace, counterName, delta, token);
	}

	@Override
	public CounterResult addAndGet(String namespace, String counterName, long delta, IdempotencyToken token) throws CounterException {
		try {
			return manager.executeSync(commands -> {
				String snapshotKey = CounterUtils.snapshotKey(namespace, counterName);
				String deltasKey = CounterUtils.deltasKey(namespace, counterName);

				if (token != null) {
					String idempotencyKey = CounterUtils.idempotencyKey(namespace, counterName, token);
					if (commands.exists(idempotencyKey) > 0) {
						long reconciled = reconcile(commands, snapshotKey, deltasKey);
						return new CounterResult(reconciled, Instant.now(), CounterConsistency.ACCURATE, token);
					}
					commands.set(idempotencyKey, "1");
				}

				commands.hincrby(deltasKey, nodeId, delta);

				long newValue = reconcile(commands, snapshotKey, deltasKey);
				return new CounterResult(newValue, Instant.now(), CounterConsistency.ACCURATE, token);
			});
		} catch (Exception exception) {
			log.error("AccurateCounter addAndGet failed", exception);
			throw new CounterException("Failed to addAndGet accurate counter", "REDIS_ERROR", exception);
		}
	}

	@Override
	public CounterResult get(String namespace, String counterName) throws CounterException {
		try {
			return manager.executeSync(commands -> {
				String snapshotKey = CounterUtils.snapshotKey(namespace, counterName);
				String deltasKey = CounterUtils.deltasKey(namespace, counterName);

				long reconciled = reconcile(commands, snapshotKey, deltasKey);
				return new CounterResult(reconciled, Instant.now(), CounterConsistency.ACCURATE, null);
			});
		} catch (Exception exception) {
			log.error("AccurateCounter get failed", exception);
			throw new CounterException("Failed to get accurate counter", "REDIS_ERROR", exception);
		}
	}

	@Override
	public void clear(String namespace, String counterName, IdempotencyToken token) throws CounterException {
		try {
			manager.executeSync(commands -> {
				String snapshotKey = CounterUtils.snapshotKey(namespace, counterName);
				String deltasKey = CounterUtils.deltasKey(namespace, counterName);

				if (token != null) {
					String idempotencyKey = CounterUtils.idempotencyKey(namespace, counterName, token);
					if (commands.exists(idempotencyKey) > 0) {
						return null;
					}
					commands.set(idempotencyKey, "1");
				}

				commands.set(snapshotKey, "0");
				commands.del(deltasKey);
				return null;
			});
		} catch (Exception exception) {
			log.error("AccurateCounter clear failed", exception);
			throw new CounterException("Failed to clear accurate counter", "REDIS_ERROR", exception);
		}
	}

	private long reconcile(
			RedisCommands<String, String> commands,
			String snapshotKey,
			String deltasKey
	) {
		long snapshot = CounterUtils.parseLong(commands.get(snapshotKey));

		Map<String, String> deltas = commands.hgetall(deltasKey);
		long deltaSum = deltas.values()
				.stream()
				.mapToLong(CounterUtils::parseLong)
				.sum();

		if (deltaSum != 0) {
			snapshot += deltaSum;
			commands.set(snapshotKey, Long.toString(snapshot));
			commands.del(deltasKey);
			commands.set(snapshotKey + ":lastSnapshotTs", Long.toString(Instant.now().toEpochMilli()));
		}

		return snapshot;
	}
}
