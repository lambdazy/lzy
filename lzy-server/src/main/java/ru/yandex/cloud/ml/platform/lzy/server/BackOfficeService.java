package ru.yandex.cloud.ml.platform.lzy.server;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import org.hibernate.Session;
import org.hibernate.Transaction;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.TokenModel;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyBackofficeGrpc;

import javax.persistence.criteria.CriteriaQuery;
import java.util.List;
import java.util.stream.Collectors;

@Requires(beans = DbStorage.class)
public class BackOfficeService extends LzyBackofficeGrpc.LzyBackofficeImplBase {
    @Inject
    DbStorage storage;

    @Inject
    Authenticator auth;

    @Override
    public void addToken(BackOffice.AddTokenRequest request, StreamObserver<BackOffice.AddTokenResult> responseObserver){
        try {
            authBackofficeCredentials(request.getBackofficeCredentials());
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

    private void authBackofficeCredentials(IAM.UserCredentials credentials) throws StatusException {
        if (!auth.checkUser(credentials.getUserId(), credentials.getToken())){
            throw Status.PERMISSION_DENIED.asException();
        }
        if (!auth.hasPermission(credentials.getUserId(), Permissions.BACKOFFICE_INTERNAL)){
            throw Status.PERMISSION_DENIED.asException();
        }
    }
}