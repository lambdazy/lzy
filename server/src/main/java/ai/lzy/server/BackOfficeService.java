package ai.lzy.server;

import ai.lzy.model.utils.AuthProvider;
import ai.lzy.model.utils.Permissions;
import ai.lzy.server.configs.ServerConfig;
import ai.lzy.server.hibernate.DbStorage;
import ai.lzy.server.hibernate.UserVerificationType;
import ai.lzy.server.hibernate.models.BackofficeSessionModel;
import ai.lzy.server.hibernate.models.PublicKeyModel;
import ai.lzy.server.hibernate.models.UserModel;
import ai.lzy.server.hibernate.models.UserRoleModel;
import ai.lzy.v1.deprecated.BackOffice;
import ai.lzy.v1.deprecated.LzyAuth;
import ai.lzy.v1.deprecated.LzyBackofficeGrpc;
import ai.lzy.v1.deprecated.LzyTask;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import org.hibernate.Session;
import org.hibernate.Transaction;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaQuery;

@Requires(beans = DbStorage.class)
public class BackOfficeService extends LzyBackofficeGrpc.LzyBackofficeImplBase {

    @Inject
    DbStorage storage;

    @Inject
    ServerConfig serverConfig;

    @Inject
    Authenticator auth;

    @Inject
    TasksManager tasks;

    @Override
    public void addKey(BackOffice.AddKeyRequest request, StreamObserver<BackOffice.AddKeyResult> responseObserver) {
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
            authBackofficeUserCredentials(request.getUserCredentials());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            UserModel user = session.get(UserModel.class, request.getUserCredentials().getUserId());
            if (user == null) {
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
                return;
            }
            PublicKeyModel token = new PublicKeyModel(request.getKeyName(), request.getPublicKey(), user);
            try {
                session.save(token);
                tx.commit();
                responseObserver.onNext(BackOffice.AddKeyResult.newBuilder().build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                tx.rollback();
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
        }
    }

    @Override
    public void createUser(BackOffice.CreateUserRequest request,
        StreamObserver<BackOffice.CreateUserResult> responseObserver) {
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
            authBackofficeUserCredentials(request.getCreatorCredentials());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }

        if (!auth.hasPermission(request.getCreatorCredentials().getUserId(), Permissions.USERS_CREATE)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }

        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                createUser(session, request.getUser().getUserId());
                tx.commit();
                responseObserver.onNext(BackOffice.CreateUserResult.newBuilder().build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                tx.rollback();
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
        }
    }

    @Override
    public void deleteUser(BackOffice.DeleteUserRequest request,
        StreamObserver<BackOffice.DeleteUserResult> responseObserver) {
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
            authBackofficeUserCredentials(request.getDeleterCredentials());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }

        if (!auth.hasPermission(request.getDeleterCredentials().getUserId(), Permissions.USERS_DELETE)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }

        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            UserModel user = session.find(UserModel.class, request.getUserId());
            try {
                session.remove(user);
                tx.commit();
                responseObserver.onNext(BackOffice.DeleteUserResult.newBuilder().build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                tx.rollback();
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
        }
    }

    @Override
    public void listUsers(BackOffice.ListUsersRequest request,
        StreamObserver<BackOffice.ListUsersResponse> responseObserver) {
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
            authBackofficeUserCredentials(request.getCallerCredentials());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }

        if (!auth.hasPermission(request.getCallerCredentials().getUserId(), Permissions.USERS_LIST)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }

        try (Session session = storage.getSessionFactory().openSession()) {
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
            } catch (Exception e) {
                tx.rollback();
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
        }
    }

    @Override
    public void generateSessionId(BackOffice.GenerateSessionIdRequest request,
        StreamObserver<BackOffice.GenerateSessionIdResponse> responseObserver) {
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }

        String sessionId = "session-" + UUID.randomUUID();

        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                session.save(new BackofficeSessionModel(sessionId, null));
                responseObserver.onNext(
                    BackOffice.GenerateSessionIdResponse.newBuilder()
                        .setSessionId(sessionId)
                        .build()
                );
                responseObserver.onCompleted();
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
        }
    }

    @Override
    public void authUserSession(BackOffice.AuthUserSessionRequest request,
        StreamObserver<BackOffice.AuthUserSessionResponse> responseObserver) {
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                UserModel user = session.find(UserModel.class, request.getUserId());
                if (user == null) {
                    user = createUser(
                        session,
                        request.getUserId()
                    );
                    session.save(user);
                }
                if (user.getAuthProvider() != null) {
                    if (AuthProvider.fromProto(request.getProvider()) != user.getAuthProviderEnum()
                        || !request.getProviderUserId().equals(user.getProviderUserId())) {
                        responseObserver.onError(Status.PERMISSION_DENIED.asException());
                        tx.rollback();
                        return;
                    }
                } else {
                    user.setAuthProviderEnum(AuthProvider.fromProto(request.getProvider()));
                    user.setProviderUserId(request.getProviderUserId());
                    session.save(user);
                }
                BackofficeSessionModel sessionModel =
                    session.find(BackofficeSessionModel.class, request.getSessionId());
                sessionModel.setOwner(user);
                session.save(sessionModel);
                responseObserver.onNext(
                    BackOffice.AuthUserSessionResponse
                        .newBuilder()
                        .setCredentials(BackOffice.BackofficeUserCredentials.newBuilder()
                            .setSessionId(sessionModel.getId())
                            .setUserId(user.getUserId())
                            .build())
                        .build()
                );
                tx.commit();
                responseObserver.onCompleted();
            } catch (Exception e) {
                tx.rollback();
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
        }

    }

    @Override
    public void checkSession(BackOffice.CheckSessionRequest request,
        StreamObserver<BackOffice.CheckSessionResponse> responseObserver) {
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        try (Session session = storage.getSessionFactory().openSession()) {
            BackofficeSessionModel sessionModel =
                session.find(BackofficeSessionModel.class, request.getSessionId());
            if (sessionModel == null) {
                responseObserver.onNext(BackOffice.CheckSessionResponse.newBuilder()
                    .setStatus(BackOffice.CheckSessionResponse.SessionStatus.NOT_EXISTS).build());
                return;
            }
            if (sessionModel.getOwner() == null) {
                responseObserver.onNext(BackOffice.CheckSessionResponse.newBuilder()
                    .setStatus(BackOffice.CheckSessionResponse.SessionStatus.NOT_RELATED_WITH_USER).build());
                return;
            }
            if (!sessionModel.getOwner().getUserId().equals(request.getUserId())) {
                responseObserver.onNext(BackOffice.CheckSessionResponse.newBuilder()
                    .setStatus(BackOffice.CheckSessionResponse.SessionStatus.WRONG_USER).build());
                return;
            }
            responseObserver.onNext(BackOffice.CheckSessionResponse.newBuilder()
                .setStatus(BackOffice.CheckSessionResponse.SessionStatus.EXISTS).build());
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void checkPermission(BackOffice.CheckPermissionRequest request,
        StreamObserver<BackOffice.CheckPermissionResponse> responseObserver) {
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
            authBackofficeUserCredentials(request.getCredentials());
        } catch (StatusException e) {
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
    public void getTasks(BackOffice.GetTasksRequest request,
        StreamObserver<BackOffice.GetTasksResponse> responseObserver) {
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
            authBackofficeUserCredentials(request.getCredentials());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        responseObserver.onNext(
            BackOffice.GetTasksResponse.newBuilder().setTasks(LzyTask.TasksList.newBuilder().addAllTasks(tasks.tasks()
                .filter(t -> this.auth.canAccess(t, request.getCredentials().getUserId()))
                .map(t -> LzyServer.Impl.taskStatus(t, tasks))
                .collect(Collectors.toList())
            )).build());

        responseObserver.onCompleted();
    }

    public void listKeys(BackOffice.ListKeysRequest request,
        StreamObserver<BackOffice.ListKeysResponse> responseObserver) {
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
            authBackofficeUserCredentials(request.getCredentials());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }

        try (Session session = storage.getSessionFactory().openSession()) {
            UserModel user = session.find(UserModel.class, request.getCredentials().getUserId());
            responseObserver.onNext(BackOffice.ListKeysResponse.newBuilder()
                .addAllKeyNames(user.getPublicKeys().stream().map(
                    PublicKeyModel::getName
                ).collect(Collectors.toList()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void deleteKey(BackOffice.DeleteKeyRequest request,
        StreamObserver<BackOffice.DeleteKeyResponse> responseObserver) {
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
            authBackofficeUserCredentials(request.getCredentials());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }

        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                PublicKeyModel token = session.find(PublicKeyModel.class,
                    new PublicKeyModel.PublicKeyPk(request.getKeyName(), request.getCredentials().getUserId()));
                if (token == null) {
                    responseObserver.onError(Status.NOT_FOUND.asException());
                    return;
                }
                session.delete(token);
                tx.commit();
                responseObserver.onNext(BackOffice.DeleteKeyResponse.newBuilder().build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                tx.rollback();
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
        }
    }

    private void authBackofficeCredentials(LzyAuth.UserCredentials credentials) throws StatusException {
        if (!auth.checkUser(credentials.getUserId(), credentials.getToken())) {
            throw Status.PERMISSION_DENIED
                .withDescription("Wrong backoffice credentials")
                .asException();
        }
        if (!auth.hasPermission(credentials.getUserId(), Permissions.BACKOFFICE_INTERNAL)) {
            throw Status.PERMISSION_DENIED
                .withDescription("Backoffice does not have internal permission")
                .asException();
        }
    }

    private void authBackofficeUserCredentials(BackOffice.BackofficeUserCredentials credentials)
        throws StatusException {
        if (!auth.checkBackOfficeSession(credentials.getSessionId(), credentials.getUserId())) {
            throw Status.PERMISSION_DENIED
                .withDescription("Wrong user credentials")
                .asException();
        }
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    private UserModel createUser(Session session, String userId) {
        UserModel user = new UserModel(userId, generateBucket(userId), typeForNewUser(session));
        session.save(user);
        UserRoleModel role = session.find(UserRoleModel.class, "user");
        Set<UserModel> users = role.getUsers();
        users.add(user);
        role.setUsers(users);
        session.save(role);
        return user;
    }

    private String generateBucket(String userId) {
        return (userId + "-" + UUID.randomUUID().toString().substring(0, 8)).toLowerCase(Locale.ROOT);
    }

    private UserVerificationType typeForNewUser(Session session) {
        if (serverConfig.getUserLimit() == 0) {
            return UserVerificationType.ACCESS_ALLOWED;
        }
        Query query = session.createQuery("select count(*) from UserModel where accessType = :accessType");
        query.setParameter("accessType", UserVerificationType.ACCESS_ALLOWED);
        int count = (int) query.getSingleResult();
        if (count < serverConfig.getUserLimit()) {
            return UserVerificationType.ACCESS_ALLOWED;
        }
        return UserVerificationType.ACCESS_PENDING;
    }

}
