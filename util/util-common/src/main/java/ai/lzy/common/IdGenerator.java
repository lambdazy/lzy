package ai.lzy.common;

public interface IdGenerator {
    String generate(int length);

    default String generate() {
        return generate(20);
    }

    default String generate(String prefix, int length) {
        return prefix + generate(length);
    }

    default String generate(String prefix) {
        return generate(prefix, 20);
    }
}
