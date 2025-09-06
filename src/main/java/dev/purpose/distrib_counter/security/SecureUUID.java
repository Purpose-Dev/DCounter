package dev.purpose.distrib_counter.security;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Locale;
import java.util.UUID;

/**
 * Utility class for generating cryptographically secure UUIDs (version 4).
 *
 * <p>Compliant with RFC 4122. Uses ThreadLocal&lt;SecureRandom&gt; to ensure
 * both cryptographic strength and high performance in multithreaded environments.</p>
 *
 * <p>Strength can be configured via system property:
 * <code>-Dsecure_uuid.mode=STRONG</code> or <code>-Dsecure_uuid.mode=DEFAULT</code>
 * Default is STRONG when available, otherwise falls back to DEFAULT.</p>
 *
 * <p>Modes:
 * <ul>
 *     <li>{@link RandomStrength#STRONG}: uses {@code SecureRandom.getInstanceStrong()}</li>
 *     <li>{@link RandomStrength#DEFAULT}: used {@code new SecureRandom()}</li>
 * </ul></p>
 *
 * @author Riyane
 * @version 1.0.0
 */
public final class SecureUUID {
	private static final RandomStrength MODE = RandomStrength.fromProperty();
	private static final ThreadLocal<byte[]> buffer = ThreadLocal.withInitial(() -> new byte[16]);
	private static final SecureRandom SECURE_RANDOM = createSecureRandom();

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
	 * Generates a cryptographically secure random UUID (version 4)
	 *
	 * @return a new UUID (v4) compliant with RFC 4122
	 */
	public static UUID generate() {
		byte[] randomBytes = buffer.get();
		SECURE_RANDOM.nextBytes(randomBytes);

		randomBytes[6] &= 0x0f;
		randomBytes[6] |= 0x40; // version to 4
		randomBytes[8] &= 0x3f;
		randomBytes[8] |= (byte) 0x80; // RFC 4122 variant

		ByteBuffer byteBuffer = ByteBuffer.wrap(randomBytes);
		long mostSigBits = byteBuffer.getLong();
		long leastSigBits = byteBuffer.getLong();

		UUID uuid = new UUID(mostSigBits, leastSigBits);
		assert uuid.version() == 4 && uuid.variant() == 2 : "Generated UUID not RFC 4122 v4";

		return uuid;
	}

	/**
	 * Convenience method for directly getting the UUID as a String
	 *
	 * @return a Secure UUID (String representation)
	 */
	public static String generateAsString() {
		return generate().toString();
	}

	/**
	 * Enum for selecting SecureRandom strategy.
	 */
	public enum RandomStrength {
		STRONG,
		DEFAULT;

		static RandomStrength fromProperty() {
			String value = System.getProperty("secure_uuid.mode", "STRONG")
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
