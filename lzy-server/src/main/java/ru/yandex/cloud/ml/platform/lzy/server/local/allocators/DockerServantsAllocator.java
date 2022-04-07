package ru.yandex.cloud.ml.platform.lzy.server.local.allocators;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DockerClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.server.ServantsAllocatorBase;

import java.util.UUID;

public class DockerServantsAllocator extends ServantsAllocatorBase {
    private static final DockerClient DOCKER = DockerClientBuilder.getInstance().build();
    private static final Logger LOG = LogManager.getLogger(DockerServantsAllocator.class);

    public DockerServantsAllocator(Authenticator auth) {
        super(auth, 60);
    }

    @Override
    protected void requestAllocation(UUID servantId, String servantToken, Provisioning provisioning, Env env, String bucket) {

    }

    @Override
    protected void cleanup(ServantConnection s) {

    }

    @Override
    protected void terminate(ServantConnection connection) {

    }
}
