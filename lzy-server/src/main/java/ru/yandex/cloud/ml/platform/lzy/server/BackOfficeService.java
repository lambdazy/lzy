package ru.yandex.cloud.ml.platform.lzy.server;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import org.hibernate.Session;
import org.hibernate.Transaction;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.Storage;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.LzyBackofficeGrpc;

public class BackOfficeService extends LzyBackofficeGrpc.LzyBackofficeImplBase {
    @Inject
    Storage storage;

    @Override
    public void addUser(BackOffice.User user, StreamObserver<BackOffice.AddUserResult> responseObserver){
        try(Session session = storage.getSessionFactory().openSession()){
            Transaction tx = session.beginTransaction();
            try {
                session.save(new UserModel(user.getUserId(), user.getPublicKey()));
                tx.commit();
                responseObserver.onNext(BackOffice.AddUserResult.newBuilder().build());
                responseObserver.onCompleted();
            }
            catch (Exception e){
                tx.rollback();
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
        }
    }
}
