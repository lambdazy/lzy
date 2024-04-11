package ai.lzy.env;

import ai.lzy.env.base.DockerEnvDescription;
import ai.lzy.env.base.DockerEnvironment;
import ai.lzy.env.logs.LogStream;
import ai.lzy.env.logs.Logs;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class DockerOomTest {
    private static final Logs logs = new Logs();
    private static final LogStream stream = logs.empty();
    static {
        logs.init(List.of());
    }

    @Test
    public void testSimple() throws Exception {
        var dockerEnvDesc = DockerEnvDescription.newBuilder()
            .withImage("alpine:3.19.1")
            .withMemLimitMb(6L)
            .withDockerClientConfig(DefaultDockerClientConfig.createDefaultConfigBuilder().build())
            .build();

        try (var env = new DockerEnvironment(dockerEnvDesc)) {
            env.install(stream, stream);
            Assert.assertThrows(Environment.OomKilledException.class, () -> env.runProcess("/bin/sh", "-c", "tail /dev/zero").waitFor());
        }
    }
}
