package dev.purpose.distrib_counter.utils;

/**
 * Utility class for working with counters and generating unique keys for storage or processing.
 * This class is final and cannot be extended.
 *
 * @author Riyane
 * @version 0.9.5
 */
public final class CounterUtils {
	public static String key(String namespace, String counterName) {
		return "counter:%s:%s".formatted(namespace, counterName);
	}

	public static String idempotencyKey(String namespace, String counterName, IdempotencyToken token) {
		return "idempotency:%s:%s:%s".formatted(namespace, counterName, token.asString());
	}

	public static long parseLong(String redisValue) {
		if (redisValue == null)
			return 0L;
		try {
			return Long.parseLong(redisValue);
		} catch (NumberFormatException exception) {
			return 0L;
		}
	}
}
