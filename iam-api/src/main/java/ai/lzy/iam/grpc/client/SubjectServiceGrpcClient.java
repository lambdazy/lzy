package ai.lzy.iam.grpc.client;

import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.iam.clients.SubjectServiceClient;
import ai.lzy.iam.utils.GrpcConfig;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.utils.GrpcConverter;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.iam.IAM;
import ai.lzy.v1.iam.LSS;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SubjectServiceGrpcClient implements SubjectServiceClient {
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
                .withInterceptors(ClientHeaderInterceptor.header(
                        GrpcHeaders.AUTHORIZATION,
                        () -> this.tokenSupplier.get().token()));
    }

    @Override
    public SubjectServiceGrpcClient withToken(Supplier<Credentials> tokenSupplier) {
        return new SubjectServiceGrpcClient(this.channel, tokenSupplier);
    }

    @Override
    public Subject createSubject(String id, String authProvider, String providerSubjectId, SubjectType type)
            throws AuthException {
        try {
            final IAM.Subject subject = subjectService.createSubject(LSS.CreateSubjectRequest.newBuilder()
                    .setName(id)
                    .setAuthProvider(authProvider)
                    .setProviderSubjectId(providerSubjectId)
                    .setType(type.toString())
                    .build());
            return GrpcConverter.to(subject);
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

    @Override
    public Subject getSubject(String id) throws AuthException {
        try {
            final IAM.Subject subject = subjectService.getSubject(LSS.GetSubjectRequest.newBuilder().setId(id).build());
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
    public List<SubjectCredentials> listCredentials(Subject subject) throws AuthException {
        try {
            LSS.ListCredentialsResponse listCredentialsResponse = subjectService.listCredentials(
                    LSS.ListCredentialsRequest.newBuilder()
                            .setSubject(GrpcConverter.from(subject))
                            .build());
            return listCredentialsResponse.getCredentialsListList()
                    .stream()
                    .map(GrpcConverter::to)
                    .collect(Collectors.toList());
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
