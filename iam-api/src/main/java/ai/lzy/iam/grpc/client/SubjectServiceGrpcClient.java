package ai.lzy.iam.grpc.client;

import ai.lzy.iam.clients.SubjectServiceClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.utils.ProtoConverter;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthInternalException;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.iam.IAM;
import ai.lzy.v1.iam.LSS;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;

public class SubjectServiceGrpcClient implements SubjectServiceClient {
    private final String clientName;
    private final Channel channel;
    private final LzySubjectServiceGrpc.LzySubjectServiceBlockingStub subjectService;
    private final Supplier<Credentials> tokenSupplier;

    public SubjectServiceGrpcClient(String clientName, Channel channel, Supplier<Credentials> tokenSupplier) {
        this.clientName = clientName;
        this.channel = channel;
        this.tokenSupplier = tokenSupplier;
        this.subjectService = newBlockingClient(
            LzySubjectServiceGrpc.newBlockingStub(this.channel), clientName, () -> this.tokenSupplier.get().token());
    }

    public SubjectServiceGrpcClient(String clientName, Channel channel,
                                    LzySubjectServiceGrpc.LzySubjectServiceBlockingStub subjectService,
                                    Supplier<Credentials> tokenSupplier)
    {
        this.clientName = clientName;
        this.channel = channel;
        this.subjectService = subjectService;
        this.tokenSupplier = tokenSupplier;
    }

    @Override
    public SubjectServiceGrpcClient withToken(Supplier<Credentials> tokenSupplier) {
        return new SubjectServiceGrpcClient(clientName, channel, tokenSupplier);
    }

    @Override
    public SubjectServiceClient withIdempotencyKey(String idempotencyKey) {
        return new SubjectServiceGrpcClient(
            clientName,
            channel,
            GrpcUtils.withIdempotencyKey(
                GrpcUtils.newBlockingClient(
                    LzySubjectServiceGrpc.newBlockingStub(channel),
                    clientName,
                    () -> tokenSupplier.get().token()),
                idempotencyKey),
            tokenSupplier);
    }

    @Override
    public Subject createSubject(AuthProvider authProvider, String providerSubjectId, SubjectType type,
                                 SubjectCredentials... credentials) throws AuthException
    {
        if (authProvider.isInternal() && type == SubjectType.USER) {
            throw new AuthInternalException("Invalid auth provider");
        }

        try {
            final IAM.Subject subject = subjectService.createSubject(
                LSS.CreateSubjectRequest.newBuilder()
                    .setAuthProvider(authProvider.toProto())
                    .setProviderSubjectId(providerSubjectId)
                    .setType(type.toString())
                    .addAllCredentials(
                        Arrays.stream(credentials)
                            .map(ProtoConverter::from)
                            .toList())
                    .build());
            return ProtoConverter.to(subject);
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

    @Override
    public Subject getSubject(String id) throws AuthException {
        try {
            final IAM.Subject subject = subjectService.getSubject(
                LSS.GetSubjectRequest.newBuilder()
                    .setId(id)
                    .build());
            return ProtoConverter.to(subject);
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

    @Override
    public void removeSubject(Subject subject) throws AuthException {
        try {
            //Empty response, see lzy-subject-service.proto
            //noinspection ResultOfMethodCallIgnored
            subjectService.removeSubject(
                LSS.RemoveSubjectRequest.newBuilder()
                    .setSubject(ProtoConverter.from(subject))
                    .build());
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

    @Override
    public void addCredentials(Subject subject, SubjectCredentials credentials) throws AuthException {
        try {
            //Empty response, see lzy-subject-service.proto
            //noinspection ResultOfMethodCallIgnored
            subjectService.addCredentials(
                LSS.AddCredentialsRequest.newBuilder()
                    .setSubject(ProtoConverter.from(subject))
                    .setCredentials(ProtoConverter.from(credentials))
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
                    .setSubject(ProtoConverter.from(subject))
                    .build());
            return listCredentialsResponse.getCredentialsListList()
                .stream()
                .map(ProtoConverter::to)
                .collect(Collectors.toList());
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

    @Override
    public void removeCredentials(Subject subject, String name) throws AuthException {
        try {
            //Empty response, see lzy-subject-service.proto
            //noinspection ResultOfMethodCallIgnored
            subjectService.removeCredentials(
                LSS.RemoveCredentialsRequest.newBuilder()
                    .setSubject(ProtoConverter.from(subject))
                    .setCredentialsName(name)
                    .build());
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

    @Override
    @Nullable
    public Subject findSubject(AuthProvider authProvider,
                               String providerSubjectId, SubjectType type) throws AuthException
    {
        try {
            var res = subjectService.findSubject(
                LSS.FindSubjectRequest.newBuilder()
                    .setAuthProvider(authProvider.name())
                    .setProviderUserId(providerSubjectId)
                    .setSubjectType(type.name())
                    .build());
            return ProtoConverter.to(res.getSubject());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Status.NOT_FOUND.getCode())) {
                return null;
            }
            throw AuthException.fromStatusRuntimeException(e);
        }
    }
}
