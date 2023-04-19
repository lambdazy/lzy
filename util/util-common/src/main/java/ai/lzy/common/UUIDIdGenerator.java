package ai.lzy.common;

import java.util.UUID;

public class UUIDIdGenerator implements IdGenerator {
    @Override
    public String generate(int length) {
        return UUID.randomUUID().toString();
    }
}
