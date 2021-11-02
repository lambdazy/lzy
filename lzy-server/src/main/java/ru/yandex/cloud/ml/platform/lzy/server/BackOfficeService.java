package ru.yandex.cloud.ml.platform.lzy.server;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import org.hibernate.Session;
import org.hibernate.Transaction;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.Storage;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.TokenModel;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.LzyBackofficeGrpc;

@Requires(beans = DbStorage.class)
public class BackOfficeService extends LzyBackofficeGrpc.LzyBackofficeImplBase {
    @Inject
    DbStorage storage;

    @Inject
    Authenticator auth;

    @Override
    public void addToken(BackOffice.AddTokenRequest request, StreamObserver<BackOffice.AddTokenResult> responseObserver){
        if (!auth.checkUser(request.getBackofficeCredentials().getUserId(), request.getBackofficeCredentials().getToken())){
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        if (!auth.canUseRole(request.getBackofficeCredentials().getUserId(), "admin")){
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        try(Session session = storage.getSessionFactory().openSession()){
            Transaction tx = session.beginTransaction();
            UserModel user = session.get(UserModel.class, request.getUserId());
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
}
