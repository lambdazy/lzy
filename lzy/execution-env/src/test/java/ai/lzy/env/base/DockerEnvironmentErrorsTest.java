package ai.lzy.env.base;

import ai.lzy.env.Environment;
import ai.lzy.env.TestLogStreams;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import io.github.resilience4j.retry.RetryConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class DockerEnvironmentErrorsTest {

    private final DockerClient dockerClient = mock(DockerClientImpl.class);
    private MockedStatic<DockerClientImpl> mockedDockerClient;
    private PullImageResultCallback pullImageResultCallback;
    private CreateContainerCmd createContainerCmd;
    private StartContainerCmd startContainerCmd;

    @Before
    public void before() {
        mockedDockerClient = mockStatic(DockerClientImpl.class);
        mockedDockerClient.when(() -> DockerClientImpl.getInstance(any(), any())).thenReturn(dockerClient);

        var inspectImageCmd = mock(InspectImageCmd.class);
        when(dockerClient.inspectImageCmd(any())).thenReturn(inspectImageCmd);
        when(inspectImageCmd.exec()).thenThrow(new NotFoundException("not cached"));

        var pullImageCmd = mock(PullImageCmd.class);
        pullImageResultCallback = mock(PullImageResultCallback.class);
        when(dockerClient.pullImageCmd(any())).thenReturn(pullImageCmd);
        when(pullImageCmd.exec(any())).thenReturn(pullImageResultCallback);

        createContainerCmd = mock(CreateContainerCmd.class);
        when(dockerClient.createContainerCmd(any())).thenReturn(createContainerCmd);
        when(createContainerCmd.withName(any())).thenReturn(createContainerCmd);
        when(createContainerCmd.withHostConfig(any())).thenReturn(createContainerCmd);
        when(createContainerCmd.withAttachStdout(any())).thenReturn(createContainerCmd);
        when(createContainerCmd.withAttachStderr(any())).thenReturn(createContainerCmd);
        when(createContainerCmd.withTty(any())).thenReturn(createContainerCmd);
        when(createContainerCmd.withUser(any())).thenReturn(createContainerCmd);
        when(createContainerCmd.withEnv(anyList())).thenReturn(createContainerCmd);
        when(createContainerCmd.withEntrypoint(any(String[].class))).thenReturn(createContainerCmd);

        startContainerCmd = mock(StartContainerCmd.class);
        when(dockerClient.startContainerCmd(any())).thenReturn(startContainerCmd);
    }

    @After
    public void after() {
        mockedDockerClient.close();
    }

    @Test
    public void testFailPullImage() throws Exception {
        when(pullImageResultCallback.awaitCompletion()).thenThrow(new NotFoundException("not found"));

        try (var env = new DockerEnvironment(createConfig(), RetryConfig.custom().maxAttempts(1).build());
             var logs = new TestLogStreams())
        {
            env.install(logs.stdout, logs.stderr);
            Assert.fail();
        } catch (Environment.InstallationException e) {
            Assert.assertEquals("Image pull failed with error Status 404: not found", e.getMessage());
        }
    }

    @Test
    public void testFailCreateContainer() throws Exception {
        when(pullImageResultCallback.awaitCompletion()).thenReturn(new ResultCallback.Adapter<>());
        when(createContainerCmd.exec()).thenThrow(new NotFoundException("impossible"));

        try (var env = new DockerEnvironment(createConfig(), RetryConfig.custom().maxAttempts(1).build());
             var logs = new TestLogStreams())
        {
            env.install(logs.stdout, logs.stderr);
            Assert.fail();
        } catch (Environment.InstallationException e) {
            Assert.assertEquals("Container creation failed with error Status 404: impossible", e.getMessage());
        }
    }

    @Test
    public void testFailStartContainer() throws Exception {
        when(pullImageResultCallback.awaitCompletion()).thenReturn(new ResultCallback.Adapter<>());
        var resp = new CreateContainerResponse();
        resp.setId("container-1");
        when(createContainerCmd.exec()).thenReturn(resp);
        when(startContainerCmd.exec()).thenThrow(new NotFoundException("unknown"));

        try (var env = new DockerEnvironment(createConfig(), RetryConfig.custom().maxAttempts(1).build());
             var logs = new TestLogStreams())
        {
            env.install(logs.stdout, logs.stderr);
            Assert.fail();
        } catch (Environment.InstallationException e) {
            Assert.assertEquals("Environment container start failed with error Status 404: unknown", e.getMessage());
        }
    }

    private static DockerEnvDescription createConfig() {
        return new DockerEnvDescription("image", "image:latest", List.of(), false, List.of(),
            null, DefaultDockerClientConfig.createDefaultConfigBuilder().build(), "0", Set.of(), null);
    }
}
