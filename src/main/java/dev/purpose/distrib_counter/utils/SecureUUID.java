package dev.purpose.distrib_counter.utils;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Locale;
import java.util.UUID;

/**
 * Utility class for generating cryptographically secure UUIDs (version 4 & 7).
 *
 * <p>Compliant with RFC 4122 (v4 & v7 draft-19).
 * Uses ThreadLocal&lt;SecureRandom&gt; to ensure both cryptographic
 * strength and high performance in multithreaded environments.</p>
 *
 * <p>Strength can be configured via system property:
 * <code>-Dsecure_uuid.mode=STRONG</code> or <code>-Dsecure_uuid.mode=DEFAULT</code>
 * Default is STRONG when available, otherwise falls back to DEFAULT.</p>
 *
 * @author Riyane
 * @version 1.1.0
 */
public final class SecureUUID {
	private static final RandomStrength MODE = RandomStrength.fromProperty();
	private static final ThreadLocal<byte[]> V4_BUFFER = ThreadLocal.withInitial(() -> new byte[16]);
	private static final ThreadLocal<byte[]> V7_BUFFER = ThreadLocal.withInitial(() -> new byte[10]);
	private static final ThreadLocal<SecureRandom> SECURE_RANDOM =
			ThreadLocal.withInitial(SecureUUID::createSecureRandom);

	private SecureUUID() {
		throw new AssertionError("SecureUUID class - do not instantiate");
	}

	private static SecureRandom createSecureRandom() {
		if (MODE == RandomStrength.STRONG) {
			try {
				return SecureRandom.getInstanceStrong();
			} catch (NoSuchAlgorithmException exception) {
				return new SecureRandom();
			}
		} else {
			return new SecureRandom();
		}
	}

	/**
	 * Generates a cryptographically secure random UUID V4
	 *
	 * @return a new UUID (v4) compliant with RFC 4122
	 */
	public static UUID generateV4() {
		final byte[] randomBytes = V4_BUFFER.get();
		SECURE_RANDOM.get().nextBytes(randomBytes);

		randomBytes[6] &= 0x0f;
		randomBytes[6] |= 0x40; // version to 4
		randomBytes[8] &= 0x3f;
		randomBytes[8] |= (byte) 0x80; // RFC 4122 variant

		ByteBuffer byteBuffer = ByteBuffer.wrap(randomBytes);
		final long mostSigBits = byteBuffer.getLong();
		final long leastSigBits = byteBuffer.getLong();

		UUID uuid = new UUID(mostSigBits, leastSigBits);
		assert uuid.version() == 4 && uuid.variant() == 2 : "Generated UUID not RFC 4122 v4";
		return uuid;
	}

	/**
	 * Generates a cryptographically secure random UUID V7
	 *
	 * @return a new UUID (v7) compliant with RFC 4122
	 */
	public static UUID generateV7() {
		final long timestamp = System.currentTimeMillis();
		final byte[] randomBytes = V7_BUFFER.get();
		SECURE_RANDOM.get().nextBytes(randomBytes);

		long mostSigBits = (timestamp & 0xFFFFFFFFFFFFL) << 16;
		mostSigBits |= (0x7L << 12);
		mostSigBits |= ((randomBytes[0] & 0x0F) << 8) | (randomBytes[1] & 0xFF);

		long leastSigBits = 0;
		for (int i = 2; i < 10; i++) {
			leastSigBits = (leastSigBits << 8) | (randomBytes[i] & 0xFFL);
		}
		leastSigBits &= 0x3FFFFFFFFFFFFFFFL;
		leastSigBits |= 0x8000000000000000L;

		UUID uuid = new UUID(mostSigBits, leastSigBits);
		assert uuid.version() == 7 && uuid.variant() == 2 : "Generated UUID not RFC 4122 v7";
		return uuid;
	}

	/**
	 * Convenience method for directly getting the UUID v4 as a String
	 *
	 * @return a Secure UUID v4 (String representation)
	 */
	public static String generateV4AsString() {
		return generateV4().toString();
	}

	/**
	 * Convenience method for directly getting the UUID v7 as a String
	 *
	 * @return a secure UUID v7 (String representation)
	 */
	public static String generateV7AsString() {
		return generateV7().toString();
	}

	/**
	 * Enum for selecting SecureRandom strategy.
	 */
	public enum RandomStrength {
		STRONG,
		DEFAULT;

		static RandomStrength fromProperty() {
			final String value = System.getProperty("secure_uuid.mode", "STRONG")
					.toUpperCase(Locale.ROOT);

			try {
				return RandomStrength.valueOf(value);
			} catch (IllegalArgumentException exception) {
				return STRONG;
			}
		}
	}

	/**
	 * Expose the active mode (useful for diagnostics)
	 *
	 * @return the random strength mode in use
	 */
	public static RandomStrength getMode() {
		return MODE;
	}
}
