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
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public final class RedisSentinelManager implements AutoCloseable {
	private static final Logger log = LoggerFactory.getLogger(RedisSentinelManager.class);

	private final RedisClient redisClient;
	private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;

	private final Retry retry;
	private final CircuitBreaker circuitBreaker;

	private RedisSentinelManager(RedisSentinelManagerBuilder builder) {
		Objects.requireNonNull(builder.sentinels, "sentinels must not be null");
		Objects.requireNonNull(builder.masterId, "masterId must not be null");

		RedisURI.Builder redisUriBuilder = RedisURI.builder()
				.withSentinelMasterId(builder.masterId);

		builder.sentinels.forEach(sentinel -> {
			String[] parts = sentinel.split(":");
			if (parts.length != 2) {
				throw new IllegalArgumentException("Invalid sentinel definition: " + sentinel);
			}
			redisUriBuilder.withSentinel(parts[0], Integer.parseInt(parts[1]));
		});

		if (builder.password != null && !builder.password.isBlank()) {
			redisUriBuilder.withPassword(builder.password.toCharArray());
		}
		if (builder.tlsEnabled) {
			redisUriBuilder.withSsl(true);
		}
		if (builder.commandTimeout != null) {
			redisUriBuilder.withTimeout(builder.commandTimeout);
		}

		RedisURI redisURI = redisUriBuilder.build();

		this.redisClient = RedisClient.create(redisURI);
		this.redisClient.setOptions(ClientOptions.builder()
				.autoReconnect(true)
				.build()
		);

		GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(builder.maxTotalConnections);
		poolConfig.setMaxIdle(builder.maxIdleConnections);
		poolConfig.setMinIdle(builder.minIdleConnections);
		poolConfig.setMaxWait(builder.maxWait);
		poolConfig.setTestOnBorrow(true);
		poolConfig.setTestOnReturn(true);

		this.pool = ConnectionPoolSupport.createGenericObjectPool(
				() -> MasterReplica.connect(this.redisClient, RedisCodec.of(null, null), redisURI),
				poolConfig
		);

		this.retry = Retry.of("redis-retry", RetryConfig.custom()
				.maxAttempts(builder.retryAttempts)
				.waitDuration(builder.retryWait)
				.retryExceptions(Exception.class)
				.build()
		);

		this.circuitBreaker = CircuitBreaker.of("redis-cb", CircuitBreakerConfig.custom()
				.failureRateThreshold(50)
				.slowCallDurationThreshold(builder.slowCallThreshold)
				.slowCallRateThreshold(50)
				.waitDurationInOpenState(builder.circuitOpenDuration)
				.minimumNumberOfCalls(10)
				.permittedNumberOfCallsInHalfOpenState(3)
				.build()
		);

		log.info("RedisSentinelManager initialized with {} sentinels, master={}, tls={}",
				builder.sentinels, builder.masterId, builder.tlsEnabled
		);
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

	public static final class RedisSentinelManagerBuilder {
		private List<String> sentinels;
		private String masterId;
		private String password;
		private boolean tlsEnabled;
		private Duration commandTimeout = Duration.ofSeconds(2);

		private int maxTotalConnections = 50;
		private int maxIdleConnections = 20;
		private int minIdleConnections = 5;
		private Duration maxWait = Duration.ofSeconds(5);

		private int retryAttempts = 3;
		private Duration retryWait = Duration.ofMillis(200);
		private Duration slowCallThreshold = Duration.ofSeconds(2);
		private Duration circuitOpenDuration = Duration.ofSeconds(30);

		private RedisSentinelManagerBuilder() {
		}

		public RedisSentinelManagerBuilder withSentinels(List<String> sentinels) {
			this.sentinels = sentinels;
			return this;
		}

		public RedisSentinelManagerBuilder withMasterId(String masterId) {
			this.masterId = masterId;
			return this;
		}

		public RedisSentinelManagerBuilder withPassword(String password) {
			this.password = password;
			return this;
		}

		public RedisSentinelManagerBuilder withTlsEnabled(boolean tlsEnabled) {
			this.tlsEnabled = tlsEnabled;
			return this;
		}

		public RedisSentinelManagerBuilder withCommandTimeout(Duration timeout) {
			this.commandTimeout = timeout;
			return this;
		}

		public RedisSentinelManagerBuilder withMaxTotalConnections(int max) {
			this.maxTotalConnections = max;
			return this;
		}

		public RedisSentinelManagerBuilder withRetryAttempts(int attempts) {
			this.retryAttempts = attempts;
			return this;
		}

		public RedisSentinelManager build() {
			return new RedisSentinelManager(this);
		}
	}

	public static RedisSentinelManagerBuilder builder() {
		return new RedisSentinelManagerBuilder();
	}

	@FunctionalInterface
	public interface RedisAction<C, R> {
		R apply(C commands) throws Exception;
	}
}
