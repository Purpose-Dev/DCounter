package dev.purpose.distrib_counter.security;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.UUID;

/**
 * Utility class for generating cryptographically secure UUIDs (version 4).
 *
 * <p>Compliant with RFC 4122. Uses ThreadLocal<SecureRandom> to ensure
 * both cryptographic strength and high performance in multithreaded environments.</p>
 *
 * @author Riyane
 * @version 0.9.0
 */
public final class SecureUUID {
	private static final ThreadLocal<SecureRandom> randomThreadLocal = ThreadLocal.withInitial(() -> {
		try {
			return SecureRandom.getInstanceStrong();
		} catch (NoSuchAlgorithmException exception) {
			return new SecureRandom();
		}
	});

	private SecureUUID() {
		throw new AssertionError("SecureUUID class - do not instantiate");
	}

	/**
	 * Generates a cryptographically secure random UUID (version 4)
	 *
	 * @return a new UUID (v4) compliant with RFC 4122
	 */
	public static UUID generate() {
		byte[] randomBytes = new byte[16];
		randomThreadLocal.get().nextBytes(randomBytes);

		// Set version to 4
		randomBytes[6] &= 0x0f;
		randomBytes[6] |= 0x40;

		// Set variant to RFC 4122
		randomBytes[8] &= 0x3f;
		randomBytes[8] |= (byte) 0x80;

		long mostSigBits = 0;
		long leastSigBits = 0;
		for (int i = 0; i < 8; i++) {
			mostSigBits = (mostSigBits << 8) | (randomBytes[i] & 0xff);
		}
		for (int i = 8; i < 16; i++) {
			leastSigBits = (leastSigBits << 8) | (randomBytes[i] & 0xff);
		}

		return new UUID(mostSigBits, leastSigBits);
	}

	/**
	 * Convenience method for directly getting the UUID as a String
	 *
	 * @return a Secure UUID (String representation)
	 */
	public static String generateAsString() {
		return generate().toString();
	}
}
