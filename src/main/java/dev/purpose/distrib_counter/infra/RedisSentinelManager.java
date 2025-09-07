package dev.purpose.distrib_counter.infra;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StatefulRedisConnectionImpl;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

public final class RedisSentinelManager implements AutoCloseable {
	private static final Logger log = LoggerFactory.getLogger(RedisSentinelManager.class);

	private final RedisClient redisClient;
	private final GenericObjectPool<StatefulRedisConnectionImpl<String, String>> pool;

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
	}

	@Override
	public void close() throws Exception {

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

		private RedisSentinelManagerBuilder() {}

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
}
