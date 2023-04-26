package ai.lzy.worker.env;

import ai.lzy.v1.common.LME;
import ai.lzy.worker.ServiceConfig;
import ai.lzy.worker.StreamQueue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class EnvTest {
    private EnvironmentFactory factory;

    @Before
    public void before() {

        var conf = new ServiceConfig();
        conf.setGpuCount(0);

        this.factory = new EnvironmentFactory(conf);
    }

    @Test
    public void testBashEnv() throws EnvironmentInstallationException {
        var env = factory.create("tid1", "", LME.EnvSpec.newBuilder()
            .setProcessEnv(LME.ProcessEnv.newBuilder().build())
            .build(), StreamQueue.LogHandle.empty());

        Assert.assertTrue(env instanceof SimpleBashEnvironment);
        Assert.assertTrue(env.base() instanceof ProcessEnvironment);
    }

    @Test
    public void testEnvVariables() throws Exception {
        var env = factory.create("tid1", "", LME.EnvSpec.newBuilder()
            .setProcessEnv(LME.ProcessEnv.newBuilder().build())
            .putEnv("LOL", "kek")
            .build(), StreamQueue.LogHandle.empty());

        var proc = env.runProcess("echo $LOL");
        Assert.assertEquals(0, proc.waitFor());


        try (var reader = new BufferedReader(new InputStreamReader(proc.out()))) {
            Assert.assertEquals("kek", reader.readLine());
        }
    }

    @Test
    public void testDocker() throws EnvironmentInstallationException {
        var env = factory.create("tid1", "", LME.EnvSpec.newBuilder()
            .setDockerImage("ubuntu:latest")
            .setProcessEnv(LME.ProcessEnv.newBuilder().build())
            .build(), StreamQueue.LogHandle.empty());

        Assert.assertTrue(env instanceof SimpleBashEnvironment);
        Assert.assertTrue(env.base() instanceof DockerEnvironment);
    }

    @Test
    public void testConda() throws EnvironmentInstallationException {
        var env = factory.create("tid1", "", LME.EnvSpec.newBuilder()
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
            .build(), StreamQueue.LogHandle.empty());

        Assert.assertTrue(env instanceof CondaEnvironment);
    }

    @After
    public void after() {
        this.factory = null;
    }
}
