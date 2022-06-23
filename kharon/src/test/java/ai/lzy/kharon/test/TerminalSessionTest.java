package ai.lzy.kharon.test;

import ai.lzy.kharon.ServerController;
import ai.lzy.kharon.UriResolver;
import io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.Test;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class TerminalSessionTest {
    @Test
    public void testServerController()
        throws URISyntaxException, InterruptedException, ServerController.ServerControllerResetException {
        final String sessionId = "term_" + UUID.randomUUID();
        final String kharonExternalHost = "kharon.external";
        final String servantFsHost = "servant.fs.host";
        final URI externalAddress =
            new URI("kharon", null, kharonExternalHost, 10000, null, null, null);
        final URI servantFsProxyAddress =
            new URI("fs", null, servantFsHost, 10000, null, null, null);

        final UriResolver uriResolver = new UriResolver(externalAddress, servantFsProxyAddress);

        final ServerController serverController = new ServerController(sessionId, uriResolver);

        Assert.assertEquals(ServerController.State.CREATED, serverController.state());

        final String slotName = "slot";
        final Servant.SlotAttach sentAttach = Servant.SlotAttach.newBuilder()
            .setSlot(Operations.Slot.newBuilder()
                .setName(slotName)
                .build())
            .setChannel("channel")
            .setUri("fs://localhost:10000/some_slot")
            .build();
        final ForkJoinTask<?> attachTask = ForkJoinPool.commonPool().submit(() -> {
            try {
                serverController.attach(sentAttach);
            } catch (ServerController.ServerControllerResetException e) {
                throw new RuntimeException(e);
            }
        });
        final Servant.SlotDetach sentDetach = Servant.SlotDetach.newBuilder()
            .setSlot(Operations.Slot.newBuilder()
                .setName(slotName)
                .build())
            .setUri("fs://localhost:10000/some_slot")
            .build();
        final ForkJoinTask<?> detachTask = ForkJoinPool.commonPool().submit(() -> {
            try {
                serverController.detach(sentDetach);
            } catch (ServerController.ServerControllerResetException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(1000);
        Assert.assertFalse(attachTask.isDone());
        Assert.assertFalse(detachTask.isDone());

        final Servant.SlotAttach[] receivedAttach = {null};
        final Servant.SlotDetach[] receivedDetach = {null};
        final AtomicBoolean completed = new AtomicBoolean(false);
        serverController.setProgress(new StreamObserver<>() {
            @Override
            public void onNext(Servant.ServantProgress servantProgress) {
                if (servantProgress.hasAttach()) {
                    Assert.assertNull(receivedAttach[0]);
                    receivedAttach[0] = servantProgress.getAttach();
                } else if (servantProgress.hasDetach()) {
                    Assert.assertNull(receivedDetach[0]);
                    receivedDetach[0] = servantProgress.getDetach();
                } else {
                    Assert.fail();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                Assert.fail();
            }

            @Override
            public void onCompleted() {
                completed.set(true);
            }
        });
        Assert.assertEquals(ServerController.State.CONNECTED, serverController.state());
        Assert.assertThrows(IllegalStateException.class, () -> serverController.setProgress(null));

        attachTask.join();
        Assert.assertEquals(slotName, receivedAttach[0].getSlot().getName());
        final URI attachUri = URI.create(receivedAttach[0].getUri());
        Assert.assertEquals(servantFsHost, attachUri.getHost());
        Assert.assertEquals(sessionId, UriResolver.parseSessionIdFromSlotUri(attachUri));

        detachTask.join();
        Assert.assertEquals(slotName, receivedDetach[0].getSlot().getName());
        final String detachUri = receivedDetach[0].getUri();
        Assert.assertEquals(servantFsHost, URI.create(detachUri).getHost());
        Assert.assertEquals(sessionId, UriResolver.parseSessionIdFromSlotUri(URI.create(detachUri)));

        serverController.complete();
        Assert.assertEquals(ServerController.State.COMPLETED, serverController.state());
        Assert.assertTrue(completed.get());

        Assert.assertThrows(ServerController.ServerControllerResetException.class,
            () -> serverController.attach(sentAttach));
    }
}
