package ru.yandex.cloud.ml.platform.lzy.iam.grpc.client;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.clients.SubjectService;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConfig;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ClientHeaderInterceptor;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.GrpcHeaders;
import yandex.cloud.lzy.v1.IAM;
import yandex.cloud.priv.lzy.v1.LSS;
import yandex.cloud.priv.lzy.v1.LzySubjectServiceGrpc;

import java.util.function.Supplier;

public class SubjectServiceGrpcClient implements SubjectService {
    private static final Logger LOG = LogManager.getLogger(SubjectServiceGrpcClient.class);

    private final Channel channel;
    private final LzySubjectServiceGrpc.LzySubjectServiceBlockingStub subjectService;
    private final Supplier<Credentials> tokenSupplier;

    public SubjectServiceGrpcClient(GrpcConfig config, Supplier<Credentials> tokenSupplier) {
        this(
                ChannelBuilder.forAddress(config.host(), config.port())
                        .usePlaintext()
                        .enableRetry(LzySubjectServiceGrpc.SERVICE_NAME)
                        .build(),
                tokenSupplier
        );
    }

    public SubjectServiceGrpcClient(Channel channel, Supplier<Credentials> tokenSupplier) {
        this.channel = channel;
        this.tokenSupplier = tokenSupplier;
        this.subjectService = LzySubjectServiceGrpc.newBlockingStub(this.channel)
                .withInterceptors(new ClientHeaderInterceptor<>(
                        GrpcHeaders.AUTHORIZATION,
                        () -> this.tokenSupplier.get().token()));
    }

    @Override
    public SubjectServiceGrpcClient withToken(Supplier<Credentials> tokenSupplier) {
        return new SubjectServiceGrpcClient(this.channel, tokenSupplier);
    }

    @Override
    public Subject createSubject(String id, String authProvider, String providerSubjectId) throws AuthException {
        try {
            final IAM.Subject subject = subjectService.createSubject(LSS.CreateSubjectRequest.newBuilder()
                    .setName(id)
                    .setAuthProvider(authProvider)
                    .setProviderSubjectId(providerSubjectId)
                    .build());
            return GrpcConverter.to(subject);
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

    @Override
    public void removeSubject(Subject subject) throws AuthException {
        try {
            //Empty response, see lzy-subject-service.proto
            LSS.RemoveSubjectResponse response = subjectService.removeSubject(
                    LSS.RemoveSubjectRequest.newBuilder()
                            .setSubject(GrpcConverter.from(subject))
                            .build());
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

    @Override
    public void addCredentials(Subject subject, String name, String value, String type) throws AuthException {
        try {
            //Empty response, see lzy-subject-service.proto
            LSS.AddCredentialsResponse response = subjectService.addCredentials(LSS.AddCredentialsRequest.newBuilder()
                            .setSubject(GrpcConverter.from(subject))
                            .setCredentials(IAM.Credentials.newBuilder()
                                    .setName(name)
                                    .setCredentials(value)
                                    .setType(type)
                                    .build())
                    .build());
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

    @Override
    public void removeCredentials(Subject subject, String name) throws AuthException {
        try {
            //Empty response, see lzy-subject-service.proto
            LSS.RemoveCredentialsResponse response = subjectService.removeCredentials(
                    LSS.RemoveCredentialsRequest.newBuilder()
                            .setSubject(GrpcConverter.from(subject))
                            .setCredentialsName(name)
                    .build());
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }
}
