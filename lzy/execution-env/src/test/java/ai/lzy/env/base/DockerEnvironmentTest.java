package ai.lzy.env.base;

import ai.lzy.env.Environment;
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
    public void testPrepareImageNotCachedImageWithoutRightPlatform() throws Exception {
        executeTest(this::doTestPrepareImageNodCachedImageWithoutRightPlatform);
    }

    @Test
    public void testPrepareImageNodCachedImageWithoutRightPlatform2() throws Exception {
        executeTest(this::doTestPrepareImageNotCachedImageWithoutRightPlatform2);
    }

    @Test
    public void testPrepareImageNodCachedImageDockerClientException() throws Exception {
        executeTest(this::doTestPrepareImageNotCachedImageDockerClientException);
    }

    private void doTestPrepareImageCachedImage() throws Exception {
        when(inspectImageResponse.getArch()).thenReturn("amd64");
        when(inspectImageResponse.getOs()).thenReturn("linux");

        DockerEnvironment environment = createEnvironment("darwin/arm64", "linux/amd64");

        environment.pullImageIfNeeded(IMAGE, logStream, logStream);

        verify(dockerClient, never()).pullImageCmd(any());
    }

    private void doTestPrepareImageCachedImageWithNotAllowedPlatform() {
        when(inspectImageResponse.getOs()).thenReturn("not_existed_os");
        when(inspectImageResponse.getArch()).thenReturn("not_existed_arch");

        DockerEnvironment environment = createEnvironment("darwin/arm64", "linux/amd64");

        var exception = Assert.assertThrows(Environment.InstallationException.class,
                () -> environment.pullImageIfNeeded(IMAGE, logStream, logStream));
        assertNotNull(exception);
        assertEquals(
            ("Image '%s' with platform 'not_existed_os/not_existed_arch' is not in the allowed platforms" +
                " [darwin/arm64, linux/amd64]").formatted(IMAGE), exception.getMessage());

        verify(dockerClient, never()).pullImageCmd(any());
    }

    private void doTestPrepareImageNodCachedImageWithRightPlatform() throws Exception {
        mockDockerClientException("Could not pull image: no matching manifest for %s in the manifest list entries".formatted(IMAGE));
        mockNotFound();
        DockerEnvironment environment = createEnvironment("darwin/arm64", "linux/amd64");

        environment.pullImageIfNeeded(IMAGE, logStream, logStream);
        verify(dockerClient, times(2)).pullImageCmd(IMAGE);
    }

    private void doTestPrepareImageNodCachedImageWithoutRightPlatform() {
        mockDockerClientException("Could not pull image: no matching manifest for %s in the manifest list entries".formatted(IMAGE));
        mockNotFound();
        DockerEnvironment environment = createEnvironment("darwin/arm64", "linux/win32");

        var exception = Assert.assertThrows(Environment.InstallationException.class,
                () -> environment.pullImageIfNeeded(IMAGE, logStream, logStream));
        assertNotNull(exception);
        assertEquals(
            "Image '%s' for platforms [linux/win32, darwin/arm64] not found".formatted(IMAGE),
            exception.getMessage());

        verify(dockerClient, times(2)).pullImageCmd(IMAGE);
    }

    private void doTestPrepareImageNotCachedImageWithoutRightPlatform2() {
        mockDockerClientException("""
                com.github.dockerjava.api.exception.DockerClientException: Could not pull image: image with reference %s
                 was found but does not match the specified platform: wanted darwin/arm64,
                 actual: linux/amd64""".formatted(IMAGE));
        mockNotFound();

        DockerEnvironment environment = createEnvironment("darwin/arm64", "linux/win32");

        var exception = Assert.assertThrows(Environment.InstallationException.class,
                () -> environment.pullImageIfNeeded(IMAGE, logStream, logStream));
        assertNotNull(exception);
        assertEquals(
            "Image '%s' for platforms [linux/win32, darwin/arm64] not found".formatted(IMAGE),
            exception.getMessage());

        verify(dockerClient, times(2)).pullImageCmd(IMAGE);
    }

    private void doTestPrepareImageNotCachedImageDockerClientException() {
        mockDockerClientException("another docker client exception");
        mockNotFound();
        DockerEnvironment environment = createEnvironment("darwin/arm64", "linux/win32");

        var exception = Assert.assertThrows(Environment.InstallationException.class,
                () -> environment.pullImageIfNeeded(IMAGE, logStream, logStream));
        assertNotNull(exception);
        assertEquals("Image pull failed", exception.getMessage());

        verify(dockerClient, times(3)).pullImageCmd(IMAGE);
    }

    private void mockDockerClientException(String message) {
        when(pullImageCmd.exec(any())).thenThrow(new DockerClientException(message));
    }

    private void mockNotFound() {
        when(inspectImageCmd.exec()).thenThrow(new NotFoundException("com.github.dockerjava.api.exception.NotFoundException:" +
                " Status 404: {\"message\":\"No such image: %s\"}\n".formatted(IMAGE)));
    }

    private DockerEnvironment createEnvironment(String... platforms) {
        return new DockerEnvironment(createDockerEnvDescription(List.of(platforms)));
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
