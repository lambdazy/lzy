package ai.lzy.iam.test;

import ai.lzy.iam.services.LzySubjectService;
import ai.lzy.v1.iam.IAM;
import ai.lzy.v1.iam.LSS;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;

import java.util.function.Consumer;

@Singleton
public class LzySubjectServiceDecorator extends LzySubjectService {
    private volatile Consumer<String> onCreate = subjId -> {};
    private volatile Consumer<String> onRemove = subjId -> {};

    @Override
    public void createSubject(LSS.CreateSubjectRequest request, StreamObserver<IAM.Subject> responseObserver) {
        super.createSubject(request, new StreamObserver<>() {
            @Override
            public void onNext(IAM.Subject subject) {
                onCreate.accept(subject.getId());
                responseObserver.onNext(subject);
            }

            @Override
            public void onError(Throwable throwable) {
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public void removeSubject(LSS.RemoveSubjectRequest request, StreamObserver<LSS.RemoveSubjectResponse> response) {
        onRemove.accept(request.getSubject().getId());
        super.removeSubject(request, response);
    }

    public LzySubjectServiceDecorator onCreate(Consumer<String> action) {
        onCreate = action;
        return this;
    }

    public LzySubjectServiceDecorator onRemove(Consumer<String> action) {
        onRemove = action;
        return this;
    }
}
