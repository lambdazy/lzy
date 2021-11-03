package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

public class KuberTask extends BaseTask {
    private static final Logger LOG = LoggerFactory.getLogger(KuberTask.class);

    KuberTask(String owner, UUID tid, Zygote workload, Map<Slot, String> assignments, ChannelsManager channels, URI serverURI) {
        super(owner, tid, workload, assignments, channels, serverURI);
    }

    @Override
    public void start(String token) {
        LOG.info("KuberTask::start {}", token);
        try {
            final ApiClient client = ClientBuilder.cluster().build();
            Configuration.setDefaultApiClient(client);

            // TODO: move path to config or env
            final File file = new File("/app/resources/kubernetes/lzy-servant-pod.yaml");  // path in docker container
            final V1Pod servantPod = (V1Pod) Yaml.load(file);

            servantPod.getSpec().getContainers().get(0).addEnvItem(
                new V1EnvVar().name("LZYTASK").value(tid.toString())
            ).addEnvItem(
                new V1EnvVar().name("LZYTOKEN").value(token)
            ).addEnvItem(
                new V1EnvVar().name("LZY_SERVER_URI").value(serverURI.toString())
            );
            servantPod.getMetadata().setName("lzy-servant-" + tid.toString().toLowerCase(Locale.ROOT));

            final CoreV1Api api = new CoreV1Api();
            final V1Pod createResult = api.createNamespacedPod("default", servantPod, null, null, null);
            LOG.info("Created servant pod in Kuber: {}", createResult);
        } catch (IOException e) {
            LOG.error("KuberTask:: IO exception while pod creation. " + e.getMessage());
        } catch (ApiException e) {
            LOG.error("KuberTask:: API exception while pod creation. " + e.getMessage());
            LOG.error(e.getResponseBody());
        } finally {

            LOG.info("KuberTask servant exited");
            try {
//                TODO: replace with container waiting
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                LOG.error("30s sleep interrupted");
            }
            LOG.info("DESTROYING STATE");
            state(State.DESTROYED);
        }
    }
}
