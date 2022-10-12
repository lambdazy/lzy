package ai.lzy.iam.grpc.client;

import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.iam.utils.ProtoConverter;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.iam.LAS;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class AuthenticateServiceGrpcClient implements AuthenticateService {
    private static final Logger LOG = LogManager.getLogger(AuthenticateServiceGrpcClient.class);

    private final String clientName;
    private final Channel channel;

    public AuthenticateServiceGrpcClient(String clientName, GrpcConfig config) {
        this(clientName, newGrpcChannel(config.host(), config.port(), LzyAuthenticateServiceGrpc.SERVICE_NAME));
    }

    public AuthenticateServiceGrpcClient(String clientName, HostAndPort address) {
        this(clientName, newGrpcChannel(address, LzyAuthenticateServiceGrpc.SERVICE_NAME));
    }

    public AuthenticateServiceGrpcClient(String clientName, String address) {
        this(clientName, HostAndPort.fromString(address));
    }

    public AuthenticateServiceGrpcClient(String clientName, Channel channel) {
        this.clientName = clientName;
        this.channel = channel;
    }

    @Override
    public Subject authenticate(Credentials credentials) throws AuthException {
        try {
            var authenticateService = GrpcUtils.newBlockingClient(
                LzyAuthenticateServiceGrpc.newBlockingStub(channel), clientName, credentials::token);

            var subject = authenticateService.authenticate(LAS.AuthenticateRequest.getDefaultInstance());
            return ProtoConverter.to(subject);
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

}
