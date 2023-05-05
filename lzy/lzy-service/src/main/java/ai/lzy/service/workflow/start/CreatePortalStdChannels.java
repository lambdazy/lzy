package ai.lzy.service.workflow.start;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.channelmanager.ProtoConverter.makeCreateChannelCommand;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class CreatePortalStdChannels implements Supplier<StepResult> {
    private final ExecutionDao execDao;
    private final String userId;
    private final String wfName;
    private final String execId;
    private final String portalStdoutChannelName;
    private final String portalStderrChannelName;
    private final String idempotencyKey;
    private final LzyChannelManagerPrivateBlockingStub channelsClient;
    private final Consumer<String> stdoutChannelIdConsumer;
    private final Consumer<String> stderrChannelIdConsumer;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public CreatePortalStdChannels(ExecutionDao execDao, String userId, String wfName, String execId,
                                   String portalStdoutChannelName, String portalStderrChannelName,
                                   @Nullable String idempotencyKey, LzyChannelManagerPrivateBlockingStub channelsClient,
                                   Consumer<String> stdoutChannelIdConsumer, Consumer<String> stderrChannelIdConsumer,
                                   Function<StatusRuntimeException, StepResult> failAction,
                                   Logger log, String logPrefix)
    {
        this.execDao = execDao;
        this.userId = userId;
        this.wfName = wfName;
        this.execId = execId;
        this.portalStdoutChannelName = portalStdoutChannelName;
        this.portalStderrChannelName = portalStderrChannelName;
        this.idempotencyKey = idempotencyKey;
        this.channelsClient = channelsClient;
        this.stdoutChannelIdConsumer = stdoutChannelIdConsumer;
        this.stderrChannelIdConsumer = stderrChannelIdConsumer;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        log.info("{} Create portal stdout/err channels: { userId: {}, wfName: {}, execId: {}, " +
                "stdoutChannelName: {}, stderrChannelName: {} }", logPrefix, userId, wfName, execId,
            portalStdoutChannelName, portalStderrChannelName);

        final String stdoutChannelId;
        final String stderrChannelId;

        try {
            var createStdoutChannelsClient = (idempotencyKey == null) ? channelsClient :
                withIdempotencyKey(channelsClient, idempotencyKey + "_create_portal_stdout");
            stdoutChannelId = createStdoutChannelsClient.create(makeCreateChannelCommand(userId, wfName, execId,
                portalStdoutChannelName)).getChannelId();
            stdoutChannelIdConsumer.accept(stdoutChannelId);
        } catch (StatusRuntimeException sre) {
            log.error("{} Error in ChannelManager::create call for portal stdout channel: {}", logPrefix,
                sre.getMessage(), sre);
            return failAction.apply(sre);
        }

        try {
            var createStderrChannelsClient = (idempotencyKey == null) ? channelsClient :
                withIdempotencyKey(channelsClient, idempotencyKey + "_create_portal_stderr");
            stderrChannelId = createStderrChannelsClient.create(makeCreateChannelCommand(userId, wfName, execId,
                portalStderrChannelName)).getChannelId();
            stderrChannelIdConsumer.accept(stderrChannelId);
        } catch (StatusRuntimeException sre) {
            log.error("{} Error in ChannelManager::create call for portal stderr channel: {}", logPrefix,
                sre.getMessage(), sre);
            try {
                var destroyStdoutChannelsClient = (idempotencyKey == null) ? channelsClient :
                    withIdempotencyKey(channelsClient, idempotencyKey + "_destroy_portal_stdout");
                //noinspection ResultOfMethodCallIgnored
                destroyStdoutChannelsClient.destroy(LCMPS.ChannelDestroyRequest.newBuilder()
                    .setChannelId(stdoutChannelId).build());
            } catch (StatusRuntimeException e) {
                log.warn("{} Cannot destroy portal stdout channel with id='{}' after error {}: ", logPrefix,
                    stdoutChannelId, sre.getMessage(), e);
            }
            return failAction.apply(sre);
        }

        try {
            withRetries(log, () -> execDao.updateStdChannelIds(execId, stdoutChannelId, stderrChannelId, null));
        } catch (Exception e) {
            log.error("{} Cannot save portal stdout/err channels ids: {}", logPrefix, e.getMessage(), e);

            try {
                var destroyStdoutChannelsClient = (idempotencyKey == null) ? channelsClient :
                    withIdempotencyKey(channelsClient, idempotencyKey + "_destroy_portal_stdout");
                //noinspection ResultOfMethodCallIgnored
                destroyStdoutChannelsClient.destroy(LCMPS.ChannelDestroyRequest.newBuilder()
                    .setChannelId(stdoutChannelId).build());
            } catch (StatusRuntimeException sre) {
                log.warn("{} Cannot destroy portal stdout channel with id='{}' after error {}: ", logPrefix,
                    stdoutChannelId, e.getMessage(), sre);
            }

            try {
                var destroyStderrChannelsClient = (idempotencyKey == null) ? channelsClient :
                    withIdempotencyKey(channelsClient, idempotencyKey + "_destroy_portal_stderr");
                //noinspection ResultOfMethodCallIgnored
                destroyStderrChannelsClient.destroy(LCMPS.ChannelDestroyRequest.newBuilder()
                    .setChannelId(stderrChannelId).build());
            } catch (StatusRuntimeException sre) {
                log.warn("{} Cannot destroy portal stderr channel with id='{}' after error {}: ", logPrefix,
                    stderrChannelId, e.getMessage(), sre);
            }

            return failAction.apply(Status.INTERNAL.withDescription("Cannot create stdout/err portal channels")
                .asRuntimeException());
        }

        return StepResult.CONTINUE;
    }
}
