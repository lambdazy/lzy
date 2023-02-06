package ai.lzy.test.impl.v2;

import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.util.auth.credentials.RsaUtils;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Singleton
public class PythonContext extends PythonContextBase {

    private final static Path file = Path.of(System.getProperty("user.dir"), "../lzy-test-cert.pem");

    public PythonContext(WorkflowContext workflow, WhiteboardContext whiteboard, IamContext iam, ServiceConfig cfg)
            throws IOException, InterruptedException
    {
        super(workflow.address().toString(), whiteboard.publicAddress().toString(),
            "test", file.toAbsolutePath().toString());

        var client = new SubjectServiceGrpcClient(
            "subject-service",
            new GrpcConfig(iam.address().getHost(), iam.address().getPort()),
            () -> cfg.getIam().createRenewableToken().get()
        );

        var keys = RsaUtils.generateRsaKeys();

        if (Files.exists(file)) {
            Files.delete(file);
        }

        Files.createFile(file);
        FileUtils.write(file.toFile(), keys.privateKey(), StandardCharsets.UTF_8);


        client.createSubject(
            AuthProvider.GITHUB, "test", SubjectType.USER, SubjectCredentials.publicKey("test", keys.publicKey()));
    }

    @PreDestroy
    public void close() {
        try {
            Files.delete(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
