package ai.lzy.service.operations.stop;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.PortalDescription;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.portal.LzyPortalGrpc.LzyPortalBlockingStub;
import org.apache.logging.log4j.Logger;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;

final class PreparePortalCleaning implements Supplier<StepResult> {
    private final ExecutionDao execDao;
    private final String execId;
    private final RenewableJwt internalUserCredentials;
    private final Consumer<PortalDescription> portalDescConsumer;
    private final Consumer<LzyPortalBlockingStub> portalClientConsumer;
    private final Consumer<LongRunningServiceBlockingStub> portalOpClientConsumer;
    private final Logger log;
    private final String logPrefix;

    public PreparePortalCleaning(ExecutionDao execDao, String execId, RenewableJwt internalUserCredentials,
                                 Consumer<PortalDescription> portalDescConsumer,
                                 Consumer<LzyPortalBlockingStub> portalClientConsumer,
                                 Consumer<LongRunningServiceBlockingStub> portalOpClientConsumer,
                                 Logger log, String logPrefix)
    {
        this.execDao = execDao;
        this.execId = execId;
        this.internalUserCredentials = internalUserCredentials;
        this.portalDescConsumer = portalDescConsumer;
        this.portalClientConsumer = portalClientConsumer;
        this.portalOpClientConsumer = portalOpClientConsumer;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        log.info("{} Prepare execution to finish: { execId: {} }", logPrefix, execId);

        PortalDescription portalDescription;
        try {
            portalDescription = withRetries(log, () -> execDao.getPortalDescription(execId));
        } catch (Exception e) {
            log.error("{} Cannot get portal description from dao: {}", logPrefix, e.getMessage(), e);
            return StepResult.RESTART;
        }

        if (portalDescription == null) {
            log.debug("{} Portal description is null, skip portal clients creation...", logPrefix);
            return StepResult.CONTINUE;
        }

        portalDescConsumer.accept(portalDescription);

        if (portalDescription.vmAddress() != null) {
            var portalChannel = newGrpcChannel(portalDescription.vmAddress(), LzyPortalGrpc.SERVICE_NAME);

            var portalClient = newBlockingClient(LzyPortalGrpc.newBlockingStub(portalChannel), APP,
                () -> internalUserCredentials.get().token());

            var portalOpClient = newBlockingClient(LongRunningServiceGrpc.newBlockingStub(portalChannel), APP,
                () -> internalUserCredentials.get().token());

            portalClientConsumer.accept(portalClient);
            portalOpClientConsumer.accept(portalOpClient);
        }

        return StepResult.CONTINUE;
    }
}
