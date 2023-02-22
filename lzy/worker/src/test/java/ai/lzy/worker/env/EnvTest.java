package ai.lzy.worker.env;

import ai.lzy.v1.common.LME;
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
        this.factory = new EnvironmentFactory("", 0);
    }

    @Test
    public void testBashEnv() {
        var env = factory.create("", LME.EnvSpec.newBuilder()
            .setProcessEnv(LME.ProcessEnv.newBuilder().build())
            .build());

        Assert.assertTrue(env instanceof SimpleBashEnvironment);
        Assert.assertTrue(env.base() instanceof ProcessEnvironment);
    }

    @Test
    public void testEnvVariables() throws Exception {
        var env = factory.create("", LME.EnvSpec.newBuilder()
            .setProcessEnv(LME.ProcessEnv.newBuilder().build())
            .putEnv("LOL", "kek")
            .build());

        var proc = env.runProcess("echo $LOL");
        Assert.assertEquals(0, proc.waitFor());


        try (var reader = new BufferedReader(new InputStreamReader(proc.out()))) {
            Assert.assertEquals("kek", reader.readLine());
        }
    }

    @Test
    public void testDocker() {
        var env = factory.create("", LME.EnvSpec.newBuilder()
            .setDockerImage("ubuntu:latest")
            .setProcessEnv(LME.ProcessEnv.newBuilder().build())
            .build());

        Assert.assertTrue(env instanceof SimpleBashEnvironment);
        Assert.assertTrue(env.base() instanceof DockerEnvironment);
    }

    @Test
    public void testConda() {
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
            .build());

        Assert.assertTrue(env instanceof CondaEnvironment);
    }

    @After
    public void after() {
        this.factory = null;
    }
}