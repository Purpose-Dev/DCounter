package dev.purpose.distrib_counter.core;

/**
 * Levels of consistency guarantees provided by {@link Counter} implementations.
 *
 * <ul>
 *     <li>{@link #BEST_EFFORT}: Operation returns the raw infrastructure result
 *     (e.g., Redis INCR) without global ordering or strong consistency</li>
 *     <li>{@link #EVENTUALLY_CONSISTENT}: Values converge asynchronously.
 *     Reads may lag behind writes but eventually reach correctness.</li>
 *     <li>{@link #ACCURATE}: The returned value is guaranteed correct at the
 *     given {@code timestamp}. Typically, it involves rollup checkpoints and
 *     reconciliation of deltas.</li>
 * </ul>
 *
 * <p>This enum allows consumers to choose an implementation suited
 * to their risk, latency, and correctness requirements.</p>
 *
 * @author Riyane
 * @version 1.0.0
 */
public enum CounterConsistency {
	/**
	 * Best-effort value (e.g., direct Redis INCR return) with no global ordering guarantees.
	 */
	BEST_EFFORT,

	/**
	 * Eventually consistent value (e.g., last rollup + optional partials)
	 * may lag behind recent writes.
	 */
	EVENTUALLY_CONSISTENT,

	/**
	 * Accurate at the instant indicated by valueTimestamp (typically last rollup checkpoint and computed delta).
	 */
	ACCURATE
}
