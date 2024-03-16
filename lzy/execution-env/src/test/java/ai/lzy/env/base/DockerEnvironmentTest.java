package ai.lzy.env.base;

import ai.lzy.env.logs.LogStream;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.InspectImageCmd;
import com.github.dockerjava.api.command.InspectImageResponse;
import com.github.dockerjava.api.command.PullImageCmd;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.exception.DockerClientException;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.PullResponseItem;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class DockerEnvironmentTest {
    private final DockerClient dockerClient = mock(DockerClientImpl.class);

    private final InspectImageCmd inspectImageCmd = mock(InspectImageCmd.class);

    private final InspectImageResponse inspectImageResponse = mock(InspectImageResponse.class);

    private final PullImageCmd pullImageCmd = mock(PullImageCmd.class);

    private final PullImageCmd pullImageCmdForRightPlatform = mock(PullImageCmd.class);

    private final ResultCallback.Adapter<PullResponseItem> callbackAdapter = mock(ResultCallback.Adapter.class);

    private static final String IMAGE = RandomStringUtils.randomAlphanumeric(20).toLowerCase();

    private final LogStream logStream = mock(LogStream.class);

    @Before
    public void setUp() throws Exception {
        when(dockerClient.inspectImageCmd(IMAGE)).thenReturn(inspectImageCmd);
        when(inspectImageCmd.exec()).thenReturn(inspectImageResponse);
        when(dockerClient.pullImageCmd(IMAGE)).thenReturn(pullImageCmd);
        when(pullImageCmd.withPlatform("linux/amd64")).thenReturn(pullImageCmdForRightPlatform);
        when(pullImageCmd.withPlatform(anyString())).thenAnswer((arg) -> {
            if ("linux/amd64".equals(arg.getArguments()[0])) {
                return pullImageCmdForRightPlatform;
            } else {
                return pullImageCmd;
            }
        });
        PullImageResultCallback pullImageResultCallback = mock(PullImageResultCallback.class);
        when(pullImageCmdForRightPlatform.exec(any())).thenReturn(pullImageResultCallback);
        when(pullImageCmd.exec(any())).thenThrow(new DockerClientException(
                "Could not pull image: no matching manifest for %s in the manifest list entries"));

        when(pullImageResultCallback.awaitCompletion()).thenReturn(callbackAdapter);
    }

    @Test
    public void testPrepareImageCachedImage() throws Exception {
        executeTest(this::doTestPrepareImageCachedImage);
    }

    @Test
    public void testPrepareImageCachedImageWithNotAllowedPlatform() throws Exception {
        executeTest(this::doTestPrepareImageCachedImageWithNotAllowedPlatform);
    }

    @Test
    public void testPrepareImageNodCachedImageWithRightPlatform() throws Exception {
        executeTest(this::doTestPrepareImageNodCachedImageWithRightPlatform);
    }

    @Test
    public void testPrepareImageNodCachedImageWithoutRightPlatform() throws Exception {
        executeTest(this::doTestPrepareImageNodCachedImageWithoutRightPlatform);
    }

    private void doTestPrepareImageCachedImage() throws Exception {
        when(inspectImageResponse.getArch()).thenReturn("amd64");
        when(inspectImageResponse.getOs()).thenReturn("linux");

        DockerEnvironment environment = new DockerEnvironment(createDockerEnvDescription(
                List.of("darwin/arm64", "linux/amd64")));

        environment.prepareImage(IMAGE, logStream);

        verify(dockerClient, never()).pullImageCmd(any());
    }

    private void doTestPrepareImageCachedImageWithNotAllowedPlatform() {
        when(inspectImageResponse.getOs()).thenReturn("not_existed_os");
        when(inspectImageResponse.getArch()).thenReturn("not_existed_arch");

        DockerEnvironment environment = new DockerEnvironment(createDockerEnvDescription(
                List.of("darwin/arm64", "linux/amd64")));

        RuntimeException exception = Assert.assertThrows(RuntimeException.class,
                () -> environment.prepareImage(IMAGE, logStream));
        assertNotNull(exception);
        assertEquals(("Image %s with platform = not_existed_os/not_existed_arch " +
                "is not in allowed platforms = darwin/arm64, linux/amd64").formatted(IMAGE), exception.getMessage());

        verify(dockerClient, never()).pullImageCmd(any());
    }

    private void doTestPrepareImageNodCachedImageWithRightPlatform() throws Exception {
        when(inspectImageCmd.exec()).thenThrow(new NotFoundException("com.github.dockerjava.api.exception.NotFoundException:" +
                " Status 404: {\"message\":\"No such image: %s\"}\n".formatted(IMAGE)));
        DockerEnvironment environment = new DockerEnvironment(createDockerEnvDescription(
                List.of("darwin/arm64", "linux/amd64")));

        environment.prepareImage(IMAGE, logStream);
        verify(dockerClient, times(2)).pullImageCmd(IMAGE);
    }

    private void doTestPrepareImageNodCachedImageWithoutRightPlatform() throws Exception {
        when(inspectImageCmd.exec()).thenThrow(new NotFoundException("com.github.dockerjava.api.exception.NotFoundException:" +
                " Status 404: {\"message\":\"No such image: %s\"}\n".formatted(IMAGE)));
        DockerEnvironment environment = new DockerEnvironment(createDockerEnvDescription(
                List.of("darwin/arm64", "linux/win32")));

        RuntimeException exception = Assert.assertThrows(RuntimeException.class,
                () -> environment.prepareImage(IMAGE, logStream));
        assertNotNull(exception);
        assertEquals("Cannot pull image for allowed platforms = linux/win32, darwin/arm64", exception.getMessage());

        verify(dockerClient, times(2)).pullImageCmd(IMAGE);
    }

    private DockerEnvDescription createDockerEnvDescription(List<String> allowedPlatforms) {
        DockerClientConfig dockerClientConfig = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
        return DockerEnvDescription.newBuilder()
                .withImage(IMAGE)
                .withDockerClientConfig(dockerClientConfig)
                .withAllowedPlatforms(allowedPlatforms)
                .build();
    }


    private void executeTest(Executable test) throws Exception {
        try (var mockedDockerClient = mockStatic(DockerClientImpl.class)) {
            mockedDockerClient.when(() -> DockerClientImpl.getInstance(any(), any())).thenReturn(dockerClient);
            test.execute();
        }
    }


    @FunctionalInterface
    private interface Executable {
        void execute() throws Exception;
    }
}
