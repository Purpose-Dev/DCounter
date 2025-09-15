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

	public static String totalKey(String namespace, String counterName) {
		return "counter:%s:%s:total".formatted(namespace, counterName);
	}

	public static String deltaKeyForNode(String namespace, String counterName, String nodeId) {
		return "counter:%s:%s:deltas:%s".formatted(namespace, counterName, nodeId);
	}

	public static String deltaKeyPattern(String namespace, String counterNamePattern) {
		return "counter:%s:%s:deltas:%s".formatted(namespace, counterNamePattern, "");
	}

	public static String snapshotKey(String namespace, String counterName) {
		return "counter:%s:%s:snapshot".formatted(namespace, counterName);
	}

	public static String deltasKey(String namespace, String counterName) {
		return "counter:%s:%s:deltas".formatted(namespace, counterName);
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
