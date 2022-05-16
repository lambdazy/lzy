package ru.yandex.cloud.ml.platform.lzy.kharon;

import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import java.net.URI;
import java.util.UUID;

public class ServerControllerFactory {
    private final LzyServerGrpc.LzyServerBlockingStub lzyServer;
    private final UriResolver uriResolver;
    private final URI kharonServantProxyUri;
    private final URI kharonServantFsProxyUri;

    public ServerControllerFactory(
        LzyServerGrpc.LzyServerBlockingStub lzyServer,
        UriResolver uriResolver,
        URI kharonServantProxyUri,
        URI kharonServantFsProxyUri
    ) {
        this.lzyServer = lzyServer;
        this.uriResolver = uriResolver;
        this.kharonServantProxyUri = kharonServantProxyUri;
        this.kharonServantFsProxyUri = kharonServantFsProxyUri;
    }

    public ServerController createInstance(
        IAM.UserCredentials auth,
        UUID sessionId
    ) {
        final IAM.UserCredentials userCredentials = IAM.UserCredentials.newBuilder()
                .setUserId(auth.getUserId())
                .setToken(auth.getToken())
                .build();

        //noinspection ResultOfMethodCallIgnored
        lzyServer.registerServant(Lzy.AttachServant.newBuilder()
            .setAuth(
                IAM.Auth.newBuilder()
                    .setUser(userCredentials)
                .build())
            .setServantURI(kharonServantProxyUri.toString())
            .setFsURI(kharonServantFsProxyUri.toString())
            .setServantId(sessionId.toString())
            .build());

        return new ServerController(sessionId, uriResolver);
    }
}
