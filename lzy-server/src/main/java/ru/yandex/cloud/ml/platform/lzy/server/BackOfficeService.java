package ru.yandex.cloud.ml.platform.lzy.server;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import org.hibernate.Session;
import org.hibernate.Transaction;
import ru.yandex.cloud.ml.platform.lzy.model.utils.AuthProviders;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.BackofficeSessionModel;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.TokenModel;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import javax.persistence.criteria.CriteriaQuery;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Requires(beans = DbStorage.class)
public class BackOfficeService extends LzyBackofficeGrpc.LzyBackofficeImplBase {
    @Inject
    DbStorage storage;

    @Inject
    Authenticator auth;

    @Inject
    TasksManager tasks;

    @Override
    public void addToken(BackOffice.AddTokenRequest request, StreamObserver<BackOffice.AddTokenResult> responseObserver){
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
            authBackofficeUserCredentials(request.getUserCredentials());
        }
        catch (StatusException e){
            responseObserver.onError(e);
            return;
        }
        try(Session session = storage.getSessionFactory().openSession()){
            Transaction tx = session.beginTransaction();
            UserModel user = session.get(UserModel.class, request.getUserCredentials().getUserId());
            if (user == null){
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
                return;
            }
            TokenModel token = new TokenModel(request.getTokenName(), request.getPublicKey(), user);
            try {
                session.save(token);
                tx.commit();
                responseObserver.onNext(BackOffice.AddTokenResult.newBuilder().build());
                responseObserver.onCompleted();
            }
            catch (Exception e){
                tx.rollback();
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
        }
    }

    @Override
    public void createUser(BackOffice.CreateUserRequest request, StreamObserver<BackOffice.CreateUserResult> responseObserver){
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
            authBackofficeUserCredentials(request.getCreatorCredentials());
        }
        catch (StatusException e){
            responseObserver.onError(e);
            return;
        }

        if (!auth.hasPermission(request.getCreatorCredentials().getUserId(), Permissions.USERS_CREATE)){
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }

        try(Session session = storage.getSessionFactory().openSession()){
            Transaction tx = session.beginTransaction();
            UserModel user = new UserModel(request.getUser().getUserId());
            try {
                session.save(user);
                tx.commit();
                responseObserver.onNext(BackOffice.CreateUserResult.newBuilder().build());
                responseObserver.onCompleted();
            }
            catch (Exception e){
                tx.rollback();
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
        }
    }

    @Override
    public void deleteUser(BackOffice.DeleteUserRequest request, StreamObserver<BackOffice.DeleteUserResult> responseObserver){
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
            authBackofficeUserCredentials(request.getDeleterCredentials());
        }
        catch (StatusException e){
            responseObserver.onError(e);
            return;
        }

        if (!auth.hasPermission(request.getDeleterCredentials().getUserId(), Permissions.USERS_DELETE)){
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }

        try(Session session = storage.getSessionFactory().openSession()){
            Transaction tx = session.beginTransaction();
            UserModel user = new UserModel(request.getUserId());
            try {
                session.remove(user);
                tx.commit();
                responseObserver.onNext(BackOffice.DeleteUserResult.newBuilder().build());
                responseObserver.onCompleted();
            }
            catch (Exception e){
                tx.rollback();
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
        }
    }

    @Override
    public void listUsers(BackOffice.ListUsersRequest request, StreamObserver<BackOffice.ListUsersResponse> responseObserver){
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
            authBackofficeUserCredentials(request.getCallerCredentials());
        }
        catch (StatusException e){
            responseObserver.onError(e);
            return;
        }

        if (!auth.hasPermission(request.getCallerCredentials().getUserId(), Permissions.USERS_LIST)){
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }

        try(Session session = storage.getSessionFactory().openSession()){
            Transaction tx = session.beginTransaction();
            try {
                CriteriaQuery<UserModel> cq = session.getCriteriaBuilder().createQuery(UserModel.class);
                List<UserModel> users = session.createQuery(cq.select(cq.from(UserModel.class))).getResultList();
                responseObserver.onNext(
                        BackOffice.ListUsersResponse.newBuilder()
                                .addAllUsers(users.stream().map(userModel ->
                                    BackOffice.User.newBuilder().setUserId(userModel.getUserId()).build()
                                ).collect(Collectors.toList()))
                                .build()
                );
                responseObserver.onCompleted();
            }
            catch (Exception e){
                tx.rollback();
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
        }
    }

    @Override
    public void generateSessionId(BackOffice.GenerateSessionIdRequest request, StreamObserver<BackOffice.GenerateSessionIdResponse> responseObserver){
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
        }
        catch (StatusException e){
            responseObserver.onError(e);
            return;
        }

        UUID sessionId = UUID.randomUUID();

        try(Session session = storage.getSessionFactory().openSession()){
            Transaction tx = session.beginTransaction();
            try {
                session.save(new BackofficeSessionModel(sessionId, null));
                responseObserver.onNext(
                        BackOffice.GenerateSessionIdResponse.newBuilder()
                                .setSessionId(sessionId.toString())
                                .build()
                );
                responseObserver.onCompleted();
                tx.commit();
            }
            catch (Exception e){
                tx.rollback();
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
        }
    }

    @Override
    public void authUserSession(BackOffice.AuthUserSessionRequest request, StreamObserver<BackOffice.AuthUserSessionResponse> responseObserver){
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
        }
        catch (StatusException e){
            responseObserver.onError(e);
            return;
        }
        try(Session session = storage.getSessionFactory().openSession()){
            Transaction tx = session.beginTransaction();
            try {
                UserModel user = session.find(UserModel.class, request.getUserId());
                if (user == null){
                    user = new UserModel(
                            request.getUserId()
                    );
                    session.save(user);
                }
                if (user.getAuthProvider() != null) {
                    if (AuthProviders.fromGrpcMessage(request.getProvider()) != user.getAuthProviderEnum() || !request.getProviderUserId().equals(user.getProviderUserId())) {
                        responseObserver.onError(Status.PERMISSION_DENIED.asException());
                        tx.rollback();
                        return;
                    }
                }
                else {
                    user.setAuthProviderEnum(AuthProviders.fromGrpcMessage(request.getProvider()));
                    user.setProviderUserId(request.getProviderUserId());
                    session.save(user);
                }
                BackofficeSessionModel sessionModel = session.find(BackofficeSessionModel.class, UUID.fromString(request.getSessionId()));
                sessionModel.setOwner(user);
                session.save(sessionModel);
                responseObserver.onNext(
                        BackOffice.AuthUserSessionResponse
                                .newBuilder()
                                .setCredentials(BackOffice.BackofficeUserCredentials.newBuilder()
                                        .setSessionId(sessionModel.getId().toString())
                                        .setUserId(user.getUserId())
                                        .build())
                                .build()
                );
                tx.commit();
                responseObserver.onCompleted();
            }
            catch (Exception e){
                tx.rollback();
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
        }

    }

    @Override
    public void checkSession(BackOffice.CheckSessionRequest request, StreamObserver<BackOffice.CheckSessionResponse> responseObserver){
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
        }
        catch (StatusException e){
            responseObserver.onError(e);
            return;
        }
        try(Session session = storage.getSessionFactory().openSession()){
            BackofficeSessionModel sessionModel = session.find(BackofficeSessionModel.class, UUID.fromString(request.getSessionId()));
            if (sessionModel == null){
                responseObserver.onNext(BackOffice.CheckSessionResponse.newBuilder().setStatus(BackOffice.CheckSessionResponse.SessionStatus.NOT_EXISTS).build());
                return;
            }
            if (sessionModel.getOwner() == null){
                responseObserver.onNext(BackOffice.CheckSessionResponse.newBuilder().setStatus(BackOffice.CheckSessionResponse.SessionStatus.NOT_RELATED_WITH_USER).build());
                return;
            }
            if (!sessionModel.getOwner().getUserId().equals(request.getUserId())){
                responseObserver.onNext(BackOffice.CheckSessionResponse.newBuilder().setStatus(BackOffice.CheckSessionResponse.SessionStatus.WRONG_USER).build());
                return;
            }
            responseObserver.onNext(BackOffice.CheckSessionResponse.newBuilder().setStatus(BackOffice.CheckSessionResponse.SessionStatus.EXISTS).build());
        }
        finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void checkPermission(BackOffice.CheckPermissionRequest request, StreamObserver<BackOffice.CheckPermissionResponse> responseObserver){
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
            authBackofficeUserCredentials(request.getCredentials());
        }
        catch (StatusException e){
            responseObserver.onError(e);
            return;
        }

        responseObserver.onNext(BackOffice.CheckPermissionResponse.newBuilder()
            .setGranted(auth.hasPermission(request.getCredentials().getUserId(), request.getPermissionName()))
            .build()
        );
        responseObserver.onCompleted();

    }

    @Override
    public void getTasks(BackOffice.GetTasksRequest request, StreamObserver<BackOffice.GetTasksResponse> responseObserver){
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
            authBackofficeUserCredentials(request.getCredentials());
        }
        catch (StatusException e){
            responseObserver.onError(e);
            return;
        }

        responseObserver.onNext(BackOffice.GetTasksResponse.newBuilder().setTasks(Tasks.TasksList.newBuilder().addAllTasks(tasks.ps()
            .filter(t -> this.auth.canAccess(t, request.getCredentials().getUserId()))
            .map(t -> LzyServer.Impl.taskStatus(t, tasks))
            .collect(Collectors.toList())
        )).build());

        responseObserver.onCompleted();

    }

    private void authBackofficeCredentials(IAM.UserCredentials credentials) throws StatusException {
        if (!auth.checkUser(credentials.getUserId(), credentials.getToken())){
            throw Status.PERMISSION_DENIED.asException();
        }
        if (!auth.hasPermission(credentials.getUserId(), Permissions.BACKOFFICE_INTERNAL)){
            throw Status.PERMISSION_DENIED.asException();
        }
    }

    private void authBackofficeUserCredentials(BackOffice.BackofficeUserCredentials credentials) throws StatusException {
        if (!auth.checkBackOfficeSession(UUID.fromString(credentials.getSessionId()), credentials.getUserId())){
            throw Status.PERMISSION_DENIED.asException();
        }
    }

}