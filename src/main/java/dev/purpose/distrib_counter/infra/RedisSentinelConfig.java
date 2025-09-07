package dev.purpose.distrib_counter.infra;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

public final class RedisSentinelConfig {
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

	private RedisSentinelConfig(RedisConfigBuilder builder) {
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

	public static RedisConfigBuilder builder() {
		return new RedisConfigBuilder();
	}

	public static final class RedisConfigBuilder {
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

		private RedisConfigBuilder() {
		}

		public RedisConfigBuilder withSentinels(List<String> sentinels) {
			this.sentinels = sentinels;
			return this;
		}

		public RedisConfigBuilder withMasterId(String masterId) {
			this.masterId = masterId;
			return this;
		}

		public RedisConfigBuilder withPassword(String password) {
			this.password = password;
			return this;
		}

		public RedisConfigBuilder withTlsEnabled(boolean tlsEnabled) {
			this.tlsEnabled = tlsEnabled;
			return this;
		}

		public RedisConfigBuilder withCommandTimeout(Duration timeout) {
			this.commandTimeout = timeout;
			return this;
		}

		public RedisConfigBuilder withMaxTotalConnections(int max) {
			this.maxTotalConnections = max;
			return this;
		}

		public RedisConfigBuilder withMaxIdleConnections(int max) {
			this.maxIdleConnections = max;
			return this;
		}

		public RedisConfigBuilder withMinIdleConnections(int min) {
			this.minIdleConnections = min;
			return this;
		}

		public RedisConfigBuilder withMaxWait(Duration maxWait) {
			this.maxWait = maxWait;
			return this;
		}

		public RedisConfigBuilder withRetryAttempts(int attempts) {
			this.retryAttempts = attempts;
			return this;
		}

		public RedisConfigBuilder withRetryWait(Duration retryWait) {
			this.retryWait = retryWait;
			return this;
		}

		public RedisConfigBuilder withSlowCallThreshold(Duration slowCallThreshold) {
			this.slowCallThreshold = slowCallThreshold;
			return this;
		}

		public RedisConfigBuilder withCircuitOpenDuration(Duration circuitOpenDuration) {
			this.circuitOpenDuration = circuitOpenDuration;
			return this;
		}

		public RedisSentinelConfig build() {
			return new RedisSentinelConfig(this);
		}
	}
}
