package auto;

public interface Runnable extends AutoCloseable {
    @Override
    void close();
}
