package ai.lzy.common;

public interface SafeCloseable extends AutoCloseable {
    @Override
    void close();
}
