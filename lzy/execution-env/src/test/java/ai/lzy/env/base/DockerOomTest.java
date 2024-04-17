package ai.lzy.env.base;

import ai.lzy.env.Environment;
import ai.lzy.env.TestLogStreams;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertThrows;

public class DockerOomTest {

    @Test
    public void testSimple() throws Exception {
        var dockerEnvDesc = DockerEnvDescription.newBuilder()
            .withImage("alpine:3.19.1")
            .withMemLimitMb(6L)
            .withDockerClientConfig(DefaultDockerClientConfig.createDefaultConfigBuilder().build())
            .build();

        try (var env = new DockerEnvironment(dockerEnvDesc);
             var logs = new TestLogStreams())
        {
            env.install(logs.stdout, logs.stderr);
            assertThrows(
                Environment.OutOfMemoryException.class,
                () -> {
                    var proc = env.runProcess("/bin/sh", "-c", """
                        A="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        for power in $(seq 100); do
                          A="${A}${A}"
                        done
                        echo $A
                        """);
                    var rc = proc.waitFor();
                    System.out.println("rc: " + rc);
                    System.out.println("OUT: " + IOUtils.toString(proc.out(), StandardCharsets.UTF_8));
                    System.err.println("ERR: " + IOUtils.toString(proc.err(), StandardCharsets.UTF_8));
                }
            );
        }
    }
}
