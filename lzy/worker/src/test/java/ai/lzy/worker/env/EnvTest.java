package ai.lzy.worker.env;

import ai.lzy.env.Environment;
import ai.lzy.env.aux.CondaEnvironment;
import ai.lzy.env.aux.SimpleBashEnvironment;
import ai.lzy.env.base.DockerEnvironment;
import ai.lzy.env.base.ProcessEnvironment;
import ai.lzy.v1.common.LME;
import ai.lzy.worker.EnvironmentFactory;
import ai.lzy.worker.LogStreams;
import ai.lzy.worker.ServiceConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

public class EnvTest {
    private EnvironmentFactory factory;
    private LogStreams logs;

    @Before
    public void before() {

        var conf = new ServiceConfig();
        conf.setGpuCount(0);

        this.factory = new EnvironmentFactory(conf);
        this.logs = new LogStreams();
        logs.init(List.of());
    }

    @Test
    public void testBashEnv() throws Environment.InstallationException {
        var env = factory.create("", LME.EnvSpec.newBuilder()
            .setProcessEnv(LME.ProcessEnv.newBuilder().build())
            .build(), "", logs);

        Assert.assertTrue(env instanceof SimpleBashEnvironment);
        Assert.assertTrue(env.base() instanceof ProcessEnvironment);
    }

    @Test
    public void testEnvVariables() throws Exception {
        var env = factory.create("", LME.EnvSpec.newBuilder()
            .setProcessEnv(LME.ProcessEnv.newBuilder().build())
            .putEnv("LOL", "kek")
            .build(), "", logs);

        var proc = env.runProcess("echo $LOL");
        Assert.assertEquals(0, proc.waitFor());


        try (var reader = new BufferedReader(new InputStreamReader(proc.out()))) {
            Assert.assertEquals("kek", reader.readLine());
        }
    }

    @Test
    public void testDocker() throws Environment.InstallationException {
        EnvironmentFactory.installEnv(false);
        var env = factory.create("", LME.EnvSpec.newBuilder()
            .setDockerImage("ubuntu:latest")
            .setProcessEnv(LME.ProcessEnv.newBuilder().build())
            .build(), "", logs);

        Assert.assertTrue(env instanceof SimpleBashEnvironment);
        Assert.assertTrue(env.base() instanceof DockerEnvironment);
        EnvironmentFactory.installEnv(true);
    }

    @Test
    public void testConda() throws Environment.InstallationException {
        EnvironmentFactory.installEnv(false);  // Do not actually install conda

        var env = factory.create("", LME.EnvSpec.newBuilder()
            .setPyenv(LME.PythonEnv.newBuilder()
                .setName("py39")
                .setYaml("""
                    name: default
                    dependencies:
                    - python=3.9.15
                    - pip
                    - cloudpickle=1.0.0
                    - pip:
                      - numpy
                      - pylzy==1.0.0
                      - serialzy>=1.0.0""")
                .build())
            .build(), "", logs);

        EnvironmentFactory.installEnv(true);

        Assert.assertTrue(env instanceof CondaEnvironment);
    }

    @After
    public void after() {
        this.factory = null;
        this.logs.close();
        this.logs = null;
    }
}
