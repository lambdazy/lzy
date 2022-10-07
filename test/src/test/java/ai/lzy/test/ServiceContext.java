package ai.lzy.test;

import com.google.common.net.HostAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ServiceContext<T> {
    private static final Logger LOG = LogManager.getLogger(ServiceContext.class);

    public record Result<T>(T stub, HostAndPort address) {}

    public interface ServiceRunner<T> {
        /**
         * Will be called while new service creating
         * Implementor must complete future with new stub and sleep forever
         * If sleep will be interrupted, implementor must clean all resources and complete function
         */
        void run(CompletableFuture<Result<T>> future);
    }

    private final CompletableFuture<Result<T>> future;
    private final Thread thread;
    private final ServiceRunner<T> runner;
    private T stub = null;
    private HostAndPort address = null;

    public ServiceContext(ServiceRunner<T> runner) {
        this.runner = runner;
        this.future = new CompletableFuture<>();
        this.thread = new Thread(() -> this.runner.run(future));
    }


    public void init() {
        this.thread.start();
        try {
            var res = this.future.get();
            this.stub = res.stub;
            this.address = res.address;
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Cannot init service", e);
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (this.thread.isAlive()) {
            this.thread.interrupt();
            try {
                this.thread.join();
            } catch (InterruptedException e) {
                LOG.error("Cannot close service", e);
            }
        }
    }

    public T getStub() {
        assert this.stub != null;
        return this.stub;
    }

    public HostAndPort getAddress() {
        assert this.address != null;
        return this.address;
    }
}
