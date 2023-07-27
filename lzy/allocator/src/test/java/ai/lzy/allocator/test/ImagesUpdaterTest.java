package ai.lzy.allocator.test;

import ai.lzy.allocator.admin.ImagesUpdater;
import ai.lzy.allocator.admin.dao.AdminDao;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.ActiveImages;
import ai.lzy.allocator.vmpool.MockMk8s;
import ai.lzy.common.RandomIdGenerator;
import io.fabric8.kubernetes.api.model.apps.DaemonSet;
import io.fabric8.kubernetes.api.model.apps.DaemonSetListBuilder;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.fabric8.kubernetes.client.utils.Serialization;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ImagesUpdaterTest {

    @Test
    public void mock() throws Exception {
        var serviceConfig = new ServiceConfig();
        serviceConfig.setHosts(List.of("localhost"));
        serviceConfig.setPort(1234);

        var kubernetesServer = new KubernetesMockServer(new MockWebServer(), new ConcurrentHashMap<>(), false);
        kubernetesServer.init(InetAddress.getLoopbackAddress(), 0);

        var daemonsets = Collections.synchronizedList(new ArrayList<DaemonSet>(2));

        kubernetesServer.expect().post()
            .withPath("/apis/apps/v1/namespaces/fictive/daemonsets")
            .andReply(HttpURLConnection.HTTP_CREATED, req -> {
                final var resource = Serialization.unmarshal(
                    new ByteArrayInputStream(req.getBody().readByteArray()), DaemonSet.class, Map.of());
                daemonsets.add(resource);
                return resource;
            })
            .always();
        kubernetesServer.expect().get()
            .withPath("/apis/apps/v1/namespaces/fictive/daemonsets")
            .andReply(HttpURLConnection.HTTP_OK, req -> new DaemonSetListBuilder()
                .addAllToItems(daemonsets)
                .build())
            .always();

        var kcf = new MockKuberClientFactory();
        kcf.setClientSupplier(kubernetesServer::createClient);

        var cr = new MockMk8s(new RandomIdGenerator());

        var updater = new ImagesUpdater(serviceConfig, kcf, cr, new DummyAdminDaoImpl());

        updater.update(new ActiveImages.Configuration(
            ActiveImages.SyncImage.of("sync1"),
            List.of(
                ActiveImages.WorkerImage.of("worker1"),
                ActiveImages.WorkerImage.of("worker2")),
            List.of(
                ActiveImages.JupyterLabImage.of("jl1", new String[0]),
                ActiveImages.JupyterLabImage.of("jl2", new String[] {"img1", "img2"}))
        ));

        Assert.assertEquals(2, daemonsets.size());

        var names = daemonsets.stream().map(ds -> ds.getMetadata().getName()).sorted().toList();
        Assert.assertEquals("worker-fictive-m", names.get(0));
        Assert.assertEquals("worker-fictive-s", names.get(1));
    }

    private static final class DummyAdminDaoImpl implements AdminDao {
        @Override
        public ActiveImages.Configuration getImages() {
            return new ActiveImages.Configuration(
                ActiveImages.SyncImage.of(null),
                List.of(),
                List.of()
            );
        }

        @Override
        public void setWorkerImages(List<ActiveImages.WorkerImage> workers) {
        }

        @Override
        public void setSyncImage(ActiveImages.SyncImage sync) {
        }

        @Override
        public void setJupyterLabImages(List<ActiveImages.JupyterLabImage> jupyterLabs) {
        }
    }
}
