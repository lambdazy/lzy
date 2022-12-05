package ai.lzy.test.impl.v2;

import ai.lzy.storage.App;
import ai.lzy.test.impl.Utils;
import com.google.common.net.HostAndPort;
import io.micronaut.context.ApplicationContext;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.util.Map;

@Singleton
public class StorageContext {
    private final HostAndPort address = HostAndPort.fromParts("localhost", 15938);
    private final ApplicationContext ctx;
    private final App storage;

    public StorageContext(IamContext iam) {

        var opts = Utils.loadModuleTestProperties("storage");
        opts.putAll(Utils.createModuleDatabase("storage"));
        opts.putAll(Map.of(
            "storage.iam.address", iam.address(),
            "storage.address", address
        ));

        ctx = ApplicationContext.run(opts);
        storage = ctx.getBean(App.class);
        try {
            storage.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public HostAndPort address() {
        return address;
    }

    @PreDestroy
    public void close() {
        storage.close(true);
        try {
            storage.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        ctx.close();
    }
}
