package dev.purpose.distrib_counter.infra;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.decorators.Decorators;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

public final class RedisSentinelManager<K, V> implements AutoCloseable {
	private static final Logger log = LoggerFactory.getLogger(RedisSentinelManager.class);

	private final RedisClient redisClient;
	private final GenericObjectPool<StatefulRedisConnection<K, V>> pool;
	private final Retry retry;
	private final CircuitBreaker circuitBreaker;

	private RedisSentinelManager(RedisSentinelConfig<K, V> config) {
		Objects.requireNonNull(config, "config must not be null");

		RedisURI.Builder redisUriBuilder = RedisURI.builder()
				.withSentinelMasterId(config.masterId());

		config.sentinels().forEach(sentinel -> {
			String[] parts = sentinel.split(":");
			if (parts.length != 2) {
				throw new IllegalArgumentException("Invalid sentinel definition: " + sentinel);
			}
			redisUriBuilder.withSentinel(parts[0], Integer.parseInt(parts[1]));
		});

		if (config.password() != null && !config.password().isBlank()) {
			redisUriBuilder.withPassword(config.password().toCharArray());
		}
		if (config.tlsEnabled()) {
			redisUriBuilder.withSsl(true);
		}
		if (config.commandTimeout() != null) {
			redisUriBuilder.withTimeout(config.commandTimeout());
		}

		RedisURI redisURI = redisUriBuilder.build();

		this.redisClient = RedisClient.create(redisURI);
		this.redisClient.setOptions(ClientOptions.builder()
				.autoReconnect(true)
				.build()
		);

		GenericObjectPoolConfig<StatefulRedisConnection<K, V>> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(config.maxTotalConnections());
		poolConfig.setMaxIdle(config.maxIdleConnections());
		poolConfig.setMinIdle(config.minIdleConnections());
		poolConfig.setMaxWait(config.maxWait());
		poolConfig.setTestOnBorrow(true);
		poolConfig.setTestOnReturn(true);

		this.pool = ConnectionPoolSupport.createGenericObjectPool(
				() -> MasterReplica.connect(
						this.redisClient,
						config.codec(),
						redisURI
				),
				poolConfig
		);

		this.retry = Retry.of("redis-retry", RetryConfig.custom()
				.maxAttempts(config.retryAttempts())
				.waitDuration(config.retryWait())
				.retryExceptions(Exception.class)
				.build()
		);

		this.circuitBreaker = CircuitBreaker.of("redis-cb", CircuitBreakerConfig.custom()
				.failureRateThreshold(50)
				.slowCallDurationThreshold(config.slowCallThreshold())
				.slowCallRateThreshold(50)
				.waitDurationInOpenState(config.circuitOpenDuration())
				.minimumNumberOfCalls(10)
				.permittedNumberOfCallsInHalfOpenState(3)
				.build()
		);

		log.info("RedisSentinelManager initialized with {} sentinels, master={}, tls={}",
				config.sentinels(), config.masterId(), config.tlsEnabled()
		);
	}

	public <T> T executeSync(RedisAction<RedisCommands<K, V>, T> action) {
		Supplier<T> decorated = Decorators.ofSupplier(() -> {
					try (StatefulRedisConnection<K, V> connection = pool.borrowObject()) {
						RedisCommands<K, V> commands = connection.sync();
						return action.apply(commands);
					} catch (Exception exception) {
						log.error("Redis operation failed", exception);
						throw new RuntimeException("Redis operation failed", exception);
					}
				}).withRetry(retry)
				.withCircuitBreaker(circuitBreaker)
				.decorate();

		return decorated.get();
	}

	public <T> CompletionStage<T> executeAsync(AsyncRedisAction<RedisAsyncCommands<K, V>, T> action) {
		final StatefulRedisConnection<K, V> connection;
		try {
			connection = pool.borrowObject();
		} catch (Exception e) {
			CompletableFuture<T> failed = new CompletableFuture<>();
			failed.completeExceptionally(new RuntimeException("Unable to borrow Redis connection from pool", e));
			return failed;
		}

		RedisAsyncCommands<K, V> asyncCommands = connection.async();
		Supplier<CompletionStage<T>> supplier = () -> {
			try {
				return action.apply(asyncCommands);
			} catch (Exception ex) {
				CompletableFuture<T> failed = new CompletableFuture<>();
				failed.completeExceptionally(ex);
				return failed;
			}
		};
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

		return getTCompletableFuture(scheduler, supplier, connection);
	}

	private <T> @NotNull CompletableFuture<T> getTCompletableFuture(
			ScheduledExecutorService scheduler,
			Supplier<CompletionStage<T>> supplier,
			StatefulRedisConnection<K, V> connection
	) {
		Supplier<CompletionStage<T>> decorated = Retry.decorateCompletionStage(retry, scheduler, () ->
				CircuitBreaker
						.decorateCompletionStage(circuitBreaker, supplier)
						.get()
		);

		CompletionStage<T> stage = decorated.get();

		CompletableFuture<T> result = new CompletableFuture<>();
		stage.whenComplete((value, error) -> {
			try {
				pool.returnObject(connection);
			} catch (Exception re) {
				log.warn("Failed to return Redis connection to pool", re);
			}

			if (error != null) {
				result.completeExceptionally(error);
			} else {
				result.complete(value);
			}
		});
		return result;
	}

	@Override
	public void close() {
		try {
			if (pool != null) {
				pool.close();
			}
			if (redisClient != null) {
				redisClient.shutdown();
			}
			log.info("RedisSentinelManager closed");
		} catch (Exception exception) {
			log.warn("Error during shutdown", exception);
		}
	}

	@FunctionalInterface
	public interface RedisAction<C, R> {
		R apply(C commands) throws Exception;
	}

	@FunctionalInterface
	public interface AsyncRedisAction<C, R> {
		CompletionStage<R> apply(C commands) throws Exception;
	}
}
