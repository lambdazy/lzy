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
import java.sql.SQLException;
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
            ActiveImages.Image.of("sync1"),
            List.of(
                ActiveImages.PoolConfig.of(List.of(), ActiveImages.DindImages.of("dind1", List.of()), "CPU", "S"),
                ActiveImages.PoolConfig.of(List.of(ActiveImages.Image.of("worker1"), ActiveImages.Image.of("worker2")), ActiveImages.DindImages.of("dind2", List.of("dindImage1")), "CPU", "m"),
                ActiveImages.PoolConfig.of(List.of(ActiveImages.Image.of("worker3")), ActiveImages.DindImages.of("dind3", List.of()), "GPU", "m"),
                ActiveImages.PoolConfig.of(List.of(), null, "CPU", "s")
        )));

        Assert.assertEquals(2, daemonsets.size());

        var names = daemonsets.stream().map(ds -> ds.getMetadata().getName()).sorted().toList();
        Assert.assertEquals("worker-fictive-m", names.get(0));
        Assert.assertEquals("worker-fictive-s", names.get(1));
    }

    private static final class DummyAdminDaoImpl implements AdminDao {
        @Override
        public void setSyncImage(ActiveImages.Image sync) throws SQLException {

        }

        @Override
        public void setImages(List<ActiveImages.PoolConfig> images) throws SQLException {

        }

        @Override
        public ActiveImages.Configuration getImages() {
            return new ActiveImages.Configuration(
                ActiveImages.Image.of(null),
                List.of()
            );
        }

    }
}
