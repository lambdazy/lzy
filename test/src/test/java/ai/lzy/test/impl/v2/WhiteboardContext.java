package ai.lzy.test.impl.v2;

import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.test.impl.Utils;
import ai.lzy.whiteboard.WhiteboardApp;
import com.google.common.net.HostAndPort;
import io.micronaut.context.ApplicationContext;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.util.Map;

@Singleton
public class WhiteboardContext {

    private final ApplicationContext context;
    private final HostAndPort address;
    private final WhiteboardApp app;

    public WhiteboardContext(IamContext iam) {
        var port = FreePortFinder.find(10000, 20000);
        address = HostAndPort.fromParts("localhost", port);

        var opts = Utils.loadModuleTestProperties("whiteboard");
        opts.putAll(Utils.createModuleDatabase("whiteboard"));
        opts.putAll(Map.of(
                "whiteboard.address", address.toString(),
                "whiteboard.iam.address", iam.address()
        ));

        context = ApplicationContext.run(opts);

        app = context.getBean(WhiteboardApp.class);
        try {
            app.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public HostAndPort publicAddress() {
        return address;
    }
    public HostAndPort privateAddress() {
        return address;
    }

    @PreDestroy
    public void stop() {
        app.stop();
        try {
            app.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        context.stop();
    }
}
