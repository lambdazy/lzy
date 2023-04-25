package ai.lzy.common;

import java.security.SecureRandom;
import java.util.random.RandomGenerator;

public class RandomIdGenerator implements IdGenerator {
    private static final String SYMBOLS = "0123456789abcdefghijklmnopqrstuvwxyz";
    private static final RandomGenerator RND = new SecureRandom();

    public String generate(int length) {
        var buf = new char[length];
        for (int i = 0; i < length; ++i) {
            buf[i] = SYMBOLS.charAt(RND.nextInt(SYMBOLS.length()));
        }
        return new String(buf);
    }
}
