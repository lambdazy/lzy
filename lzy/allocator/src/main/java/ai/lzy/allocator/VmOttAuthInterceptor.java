package ai.lzy.allocator;

import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.iam.utils.TokenParser;
import ai.lzy.model.db.DbHelper;
import ai.lzy.util.auth.credentials.OttCredentials;
import ai.lzy.util.auth.credentials.OttHelper;
import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static ai.lzy.util.grpc.GrpcHeaders.AUTHORIZATION;

final class VmOttAuthInterceptor implements ServerInterceptor {
    private static final Logger LOG = LogManager.getLogger(VmOttAuthInterceptor.class);

    private final VmDao vmDao;
    private final List<MethodDescriptor<?, ?>> methods;

    public VmOttAuthInterceptor(VmDao vmDao, MethodDescriptor<?, ?>... methods) {
        this.vmDao = vmDao;
        this.methods = List.of(methods);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next)
    {
        if (!methods.contains(call.getMethodDescriptor())) {
            return next.startCall(call, headers);
        }

        var authorizationHeader = headers.get(AUTHORIZATION);
        if (authorizationHeader == null) {
            call.close(Status.INVALID_ARGUMENT.withDescription("Authorization header is missing"), new Metadata());
            return new ServerCall.Listener<>() {};
        }

        var token = TokenParser.parse(authorizationHeader);
        var credentials = switch (token.kind()) {
            case JWT -> {
                LOG.error("Unexpected JWT auth for the `register` call");
                throw new IllegalArgumentException("OTT auth expected, got JWT");
            }
            case OTT -> new OttCredentials(token.token());
        };

        var decodedOtt = OttHelper.decodeOtt(credentials);

        LOG.debug("Authenticate VM by OTT: {}...", decodedOtt.toStringSafe());

        String vmId;
        try {
            vmId = DbHelper.withRetries(LOG, () -> vmDao.resetVmOtt(decodedOtt.ott(), null));
        } catch (Exception e) {
            LOG.error("Cannot auth VM by OTT {}: {}", credentials.token(), e.getMessage());
            call.close(Status.UNAVAILABLE.withDescription("Retry later"), new Metadata());
            return new ServerCall.Listener<>() {};
        }

        if (vmId == null) {
            try {
                var vm = DbHelper.withRetries(LOG, () -> vmDao.get(decodedOtt.subjectId(), null));
                if (vm != null && vm.vmId().equals(decodedOtt.subjectId())) {
                    LOG.error("Vm {} has been already registered", vm);
                    call.close(Status.ALREADY_EXISTS, new Metadata());
                    return new ServerCall.Listener<>() {};
                }
            } catch (Exception e) {
                LOG.error("Cannot read VM {}: {}", decodedOtt.subjectId(), e.getMessage());
            }
            LOG.error("Cannot auth VM by OTT {}: not found", credentials.token());
            call.close(Status.PERMISSION_DENIED, new Metadata());
            return new ServerCall.Listener<>() {};
        }

        if (!vmId.equals(decodedOtt.subjectId())) {
            LOG.error("Cannot auth VM by OTT, requestedVm: {}, knownVm: {}", decodedOtt.subjectId(), vmId);
            call.close(Status.PERMISSION_DENIED, new Metadata());
            return new ServerCall.Listener<>() {};
        }

        return next.startCall(call, headers);
    }
}
