package org.telegram.mtproto.secure;

import java.security.SecureRandom;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 03.11.13
 * Time: 4:05
 */
public final class Entropy {
    private static SecureRandom random = new SecureRandom();

    public static byte[] generateSeed(int size) {
        return random.generateSeed(size);
    }

    public static long generateRandomId() {
        return random.nextLong();
    }

    public static void feedEntropy(byte[] data) {
        random.setSeed(data);
    }
}
