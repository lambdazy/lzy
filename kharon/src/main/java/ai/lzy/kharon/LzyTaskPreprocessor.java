package ai.lzy.kharon;

import ai.lzy.disk.DiskType;
import ai.lzy.env.client.CachedEnvGrpcClient;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.graph.AtomicZygote;
import ai.lzy.model.graph.PythonEnv;
import ai.lzy.priv.v2.Operations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LzyTaskPreprocessor {

    private static final Logger LOG = LogManager.getLogger(LzyTaskPreprocessor.class);

    private CachedEnvGrpcClient cachedEnvClient;

    public AtomicZygote process(AtomicZygote zygote) {
        return switch (zygote.name()) {
            case "preparing-env" -> transformToPreparingEnvZygote(zygote);
            default -> zygote;
        };
    }

    private AtomicZygote transformToPreparingEnvZygote(AtomicZygote zygote) {
        LOG.info("Preparing-env zygote detected, transforming it");
        String dockerImage = zygote.env().baseEnv().name();
        String yamlConfig = ((PythonEnv) zygote.env().auxEnv()).yaml();
        String diskId = cachedEnvClient.saveEnvConfig("", dockerImage, yamlConfig, DiskType.S3_STORAGE);
        Operations.Provisioning updatedProvisioning = Operations.Provisioning.newBuilder()
            .addAllTags(
                zygote.provisioning()
                    .tags()
                    .map(tag -> Operations.Provisioning.Tag.newBuilder().setTag(tag.tag()).build())
                    .toList()
            )
            .addTags(Operations.Provisioning.Tag.newBuilder().setTag("disk:" + diskId).build())
            .build();
        AtomicZygote transformedZygote = GrpcConverter.from(Operations.Zygote.newBuilder()
            .setEnv(GrpcConverter.to(zygote.env()))
            .setProvisioning(updatedProvisioning)
            .setFuze("echo 42")
            .build());
        LOG.info("Preparing-env zygote transformed to {}", zygote);
        return transformedZygote;
    }

}
