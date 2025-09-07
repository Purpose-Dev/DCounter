package dev.purpose.distrib_counter.infra;

import io.lettuce.core.codec.RedisCodec;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Immutable configuration for Redis Sentinel connectivity and pooling.
 *
 * <p>This configuration object encapsulates all parameters required
 * for creating a {@link RedisSentinelManager}. It is generic over
 * the key/value types handled by Redis, controlled via the {@link RedisCodec}.</p>
 *
 * <p>Defaults are provided for pooling, timeouts and resilience.
 * All fields are immutable once built.</p>
 *
 * @param <K> type of Redis keys
 * @param <V> type of Redis values
 * @author Riyane
 * @version 1.0.0
 */
public final class RedisSentinelConfig<K, V> {
	private final List<String> sentinels;
	private final String masterId;
	private final String password;
	private final boolean tlsEnabled;
	private final Duration commandTimeout;

	private final int maxTotalConnections;
	private final int maxIdleConnections;
	private final int minIdleConnections;
	private final Duration maxWait;

	private final int retryAttempts;
	private final Duration retryWait;
	private final Duration slowCallThreshold;
	private final Duration circuitOpenDuration;

	private final RedisCodec<K, V> codec;

	private RedisSentinelConfig(RedisConfigBuilder<K, V> builder) {
		this.sentinels = List.copyOf(Objects.requireNonNull(builder.sentinels, "sentinels must not be null"));
		this.masterId = Objects.requireNonNull(builder.masterId, "masterId must not be null");
		this.password = builder.password;
		this.tlsEnabled = builder.tlsEnabled;
		this.commandTimeout = Objects.requireNonNullElse(builder.commandTimeout, Duration.ofSeconds(2));

		this.maxTotalConnections = builder.maxTotalConnections;
		this.maxIdleConnections = builder.maxIdleConnections;
		this.minIdleConnections = builder.minIdleConnections;
		this.maxWait = builder.maxWait;

		this.retryAttempts = builder.retryAttempts;
		this.retryWait = builder.retryWait;
		this.slowCallThreshold = builder.slowCallThreshold;
		this.circuitOpenDuration = builder.circuitOpenDuration;

		this.codec = builder.codec;
	}

	public List<String> sentinels() {
		return sentinels;
	}

	public String masterId() {
		return masterId;
	}

	public String password() {
		return password;
	}

	public boolean tlsEnabled() {
		return tlsEnabled;
	}

	public Duration commandTimeout() {
		return commandTimeout;
	}

	public int maxTotalConnections() {
		return maxTotalConnections;
	}

	public int maxIdleConnections() {
		return maxIdleConnections;
	}

	public int minIdleConnections() {
		return minIdleConnections;
	}

	public Duration maxWait() {
		return maxWait;
	}

	public int retryAttempts() {
		return retryAttempts;
	}

	public Duration retryWait() {
		return retryWait;
	}

	public Duration slowCallThreshold() {
		return slowCallThreshold;
	}

	public Duration circuitOpenDuration() {
		return circuitOpenDuration;
	}

	public RedisCodec<K, V> codec() {
		return codec;
	}

	@Override
	public String toString() {
		return "RedisSentinelConfig[" +
				"sentinels=" + sentinels +
				", masterId='" + masterId + '\'' +
				", tlsEnabled=" + tlsEnabled +
				", commandTimeout=" + commandTimeout +
				", maxTotalConnections=" + maxTotalConnections +
				", maxIdleConnections=" + maxIdleConnections +
				", minIdleConnections=" + minIdleConnections +
				", retryAttempts=" + retryAttempts +
				", retryWait=" + retryWait +
				", slowCallThreshold=" + slowCallThreshold +
				", circuitOpenDuration=" + circuitOpenDuration +
				']';
	}

	/**
	 * Creates a new instance of {@code RedisConfigBuilder} with the specified codec.
	 * This builder allows customization of Redis Sentinel configuration.
	 *
	 * @param <K>   the type of keys used in Redis.
	 * @param <V>   the type of values used in Redis.
	 * @param codec the {@code RedisCodec} used for key and value serialization. Must not be null.
	 * @return a {@code RedisConfigBuilder} instance initialized with the given codec.
	 * @throws NullPointerException if the codec is null.
	 */
	public static <K, V> RedisConfigBuilder<K, V> builder(RedisCodec<K, V> codec) {
		return new RedisConfigBuilder<>(codec);
	}

	/**
	 * Builder for {@link RedisSentinelConfig}
	 *
	 * @param <K> type of Redis keys
	 * @param <V> type of Redis values
	 */
	public static final class RedisConfigBuilder<K, V> {
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

		private RedisCodec<K, V> codec;

		private RedisConfigBuilder(RedisCodec<K, V> codec) {
			this.codec = Objects.requireNonNull(codec, "codec must not be null");
		}

		public RedisConfigBuilder<K, V> withSentinels(List<String> sentinels) {
			this.sentinels = sentinels;
			return this;
		}

		public RedisConfigBuilder<K, V> withMasterId(String masterId) {
			this.masterId = masterId;
			return this;
		}

		public RedisConfigBuilder<K, V> withPassword(String password) {
			this.password = password;
			return this;
		}

		public RedisConfigBuilder<K, V> withTlsEnabled(boolean tlsEnabled) {
			this.tlsEnabled = tlsEnabled;
			return this;
		}

		public RedisConfigBuilder<K, V> withCommandTimeout(Duration timeout) {
			this.commandTimeout = timeout;
			return this;
		}

		public RedisConfigBuilder<K, V> withMaxTotalConnections(int max) {
			this.maxTotalConnections = max;
			return this;
		}

		public RedisConfigBuilder<K, V> withMaxIdleConnections(int max) {
			this.maxIdleConnections = max;
			return this;
		}

		public RedisConfigBuilder<K, V> withMinIdleConnections(int min) {
			this.minIdleConnections = min;
			return this;
		}

		public RedisConfigBuilder<K, V> withMaxWait(Duration maxWait) {
			this.maxWait = maxWait;
			return this;
		}

		public RedisConfigBuilder<K, V> withRetryAttempts(int attempts) {
			this.retryAttempts = attempts;
			return this;
		}

		public RedisConfigBuilder<K, V> withRetryWait(Duration retryWait) {
			this.retryWait = retryWait;
			return this;
		}

		public RedisConfigBuilder<K, V> withSlowCallThreshold(Duration slowCallThreshold) {
			this.slowCallThreshold = slowCallThreshold;
			return this;
		}

		public RedisConfigBuilder<K, V> withCircuitOpenDuration(Duration circuitOpenDuration) {
			this.circuitOpenDuration = circuitOpenDuration;
			return this;
		}

		public RedisConfigBuilder<K, V> withCodec(RedisCodec<K, V> codec) {
			this.codec = codec;
			return this;
		}

		public RedisSentinelConfig<K, V> build() {
			return new RedisSentinelConfig<>(this);
		}
	}
}
