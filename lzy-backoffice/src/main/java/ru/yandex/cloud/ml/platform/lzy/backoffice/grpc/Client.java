package ru.yandex.cloud.ml.platform.lzy.backoffice.grpc;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.backoffice.CredentialsConfig;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.AddTokenRequest;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.CreateUserRequest;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.DeleteUserRequest;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.User;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.LzyBackofficeGrpc;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;


@Singleton
public class Client {
    private final Channel channel;

    @Inject
    CredentialsConfig credentials;

    @Inject
    Client(GrpcConfig config){
        System.out.println("Starting channel on "+ config.getHost() + ":" + config.getPort());

        channel = ManagedChannelBuilder.forAddress(config.getHost(), config.getPort()).usePlaintext().build();
    }

    public LzyBackofficeGrpc.LzyBackofficeBlockingStub getBlockingStub(){
        return LzyBackofficeGrpc.newBlockingStub(channel);
    }

    public LzyBackofficeGrpc.LzyBackofficeStub getAsyncStub(){
        return LzyBackofficeGrpc.newStub(channel);
    }

    public BackOffice.AddTokenResult addToken(AddTokenRequest request){
        try {
            return getBlockingStub().addToken(
                    request.getModel(credentials.getCredentials())
            );
        }
        catch (InvalidKeySpecException | NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
            throw new HttpStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Corrupted backoffice token");
        }
    }

    public BackOffice.CreateUserResult createUser(CreateUserRequest request){
        try {
            return getBlockingStub().createUser(request.getModel(credentials.getCredentials()));
        }
        catch (InvalidKeySpecException | NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
            throw new HttpStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Corrupted backoffice token");
        }
    }

    public BackOffice.DeleteUserResult deleteUser(DeleteUserRequest request){
        try {
            return getBlockingStub().deleteUser(request.getModel(credentials.getCredentials()));
        }
        catch (InvalidKeySpecException | NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
            throw new HttpStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Corrupted backoffice token");
        }
    }
}
