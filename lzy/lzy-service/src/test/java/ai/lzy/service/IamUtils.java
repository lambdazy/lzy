package ai.lzy.service;

import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.auth.credentials.RsaUtils;
import io.grpc.stub.AbstractBlockingStub;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;

public abstract class IamUtils {
    private IamUtils() {}

    public static Subject createIamSubject(String userName, String publicKey, SubjectServiceGrpcClient grpcClient) {
        return grpcClient.createSubject(AuthProvider.GITHUB, userName, SubjectType.USER,
            new SubjectCredentials("main", publicKey, CredentialsType.PUBLIC_KEY));
    }

    public static <T extends AbstractBlockingStub<T>> T authorize(T grpcClient, String userName,
                                                                  SubjectServiceGrpcClient iamGrpcClient)
        throws IOException, InterruptedException, NoSuchAlgorithmException, InvalidKeySpecException
    {
        RsaUtils.RsaKeys rsaKeys = RsaUtils.generateRsaKeys();
        Subject iamSubject = createIamSubject(userName, rsaKeys.publicKey(), iamGrpcClient);
        PrivateKey privateKey = CredentialsUtils.readPrivateKey(rsaKeys.privateKey());
        var jwt = new RenewableJwt(userName, AuthProvider.GITHUB.name(), Duration.ofHours(1), privateKey);
        return newBlockingClient(grpcClient, "TestClient", () -> jwt.get().token());
    }
}
