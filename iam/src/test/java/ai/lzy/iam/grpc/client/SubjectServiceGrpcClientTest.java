package ai.lzy.iam.grpc.client;

import ai.lzy.iam.authorization.credentials.JwtCredentials;
import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.utils.GrpcConfig;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import static ai.lzy.iam.utils.CredentialsHelper.buildJWT;

public class SubjectServiceGrpcClientTest {
    private static final Logger LOG = LogManager.getLogger(SubjectServiceGrpcClientTest.class);

    ApplicationContext ctx;
    SubjectServiceGrpcClient subjectClient;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        InternalUserConfig internalUserConfig = ctx.getBean(InternalUserConfig.class);
        subjectClient = new SubjectServiceGrpcClient(
            GrpcConfig.from("localhost:8443"),
            () -> {
                try (final Reader reader = new StringReader(internalUserConfig.credentialPrivateKey())) {
                    return new JwtCredentials(buildJWT(internalUserConfig.userName(), reader));
                } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
                    throw new RuntimeException("Cannot build credentials");
                }
            }
        );
    }


}
