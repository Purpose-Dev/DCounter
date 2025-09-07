package dev.purpose.distrib_counter.core;

/**
 * Base exception type for all distributed counter-operations.
 *
 * <p>Thrown when an operation cannot be completed successfully,
 * typically due to:</p>
 * <ul>
 *     <li>Infrastructure failure (Redis Sentinel down, broker unreachable).</li>
 *     <li>Invalid arguments (null namespace, invalid token).</li>
 *     <li>Idempotency conflict (token replay detected).</li>
 * </ul>
 *
 * <h4>Usage guidelines:</h4>
 * <ul>
 *     <li>Always log and propagate with context for monitoring.</li>
 *     <li>Clients may inspect the message and cause for troubleshooting</li>
 * </ul>
 *
 * @author Riyane
 * @version 1.0.0
 */
public class CounterException extends Exception {
	private final String errorCode;

	public CounterException(String message, String errorCode) {
		super(message);
		this.errorCode = errorCode;
	}

	public CounterException(String message, String errorCode, Throwable cause) {
		super(message, cause);
		this.errorCode = errorCode;
	}

	public String getErrorCode() {
		return errorCode;
	}
}
