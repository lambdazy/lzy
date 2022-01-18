package ru.yandex.cloud.ml.platform.lzy.servant.env;

import static org.junit.Assert.assertEquals;

import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.env.EnvConfig.MountDescription;
import ru.yandex.cloud.ml.platform.lzy.servant.env.Environment.LzyProcess;

public class EnvTest {
    @Test
    public void testEnvRunSimple()
        throws Exception {
        final BaseEnvironment env = new BaseEnvironment(new EnvConfig());

        final LzyProcess job = env.runProcess("/bin/bash", "-c", "echo 42 && echo 43 > /dev/stderr");

        final StringBuilder stderrBuilder = new StringBuilder();
        final StringBuilder stdoutBuilder = new StringBuilder();
        try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(job.err()))) {
            reader.lines().forEach(stderrBuilder::append);
        }

        try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(job.out()))) {
            reader.lines().forEach(stdoutBuilder::append);
        }

        System.out.println("Return code: " + job.waitFor());
        env.close();

        assertEquals(stdoutBuilder.toString(), "42");
        assertEquals(stderrBuilder.toString(), "43");
    }

    @Test
    public void testCondaEnv()
        throws Exception {
        final EnvConfig envConfig = new EnvConfig();
        envConfig.mounts.add(new MountDescription("/tmp/resources", "/tmp/resources"));
        final BaseEnvironment env = new BaseEnvironment(envConfig);

        try (FileWriter file = new FileWriter("/tmp/resources/conda.yaml")) {
            file.write(String.join("\n",
                "name: default",
                "dependencies:",
                "- python==3.7.11",
                "- pip",
                "- pip:",
                "  - numpy==1.21.4",
                ""));
        }
        final LzyProcess preJob = env.runProcess("bash", "-c",
            "source /root/miniconda3/etc/profile.d/conda.sh && conda activate default && conda env update --file /tmp/resources/conda.yaml"
        );
        try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(preJob.out()))) {
            reader.lines().forEach(System.out::println);
        }
        try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(preJob.err()))) {
            reader.lines().forEach(System.err::println);
        }
        assertEquals(preJob.waitFor(), 0);
        env.close();
    }
}
