package ai.lzy.common;

import java.util.Locale;
import java.util.UUID;

public class UUIDIdGenerator implements IdGenerator {
    @Override
    public String generate(int length) {
        return UUID.randomUUID().toString().toLowerCase(Locale.ROOT);
    }
}
