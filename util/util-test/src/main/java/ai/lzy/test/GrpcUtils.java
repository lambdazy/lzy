package ai.lzy.test;

import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.v1.ChannelManager;
import ai.lzy.v1.Channels;
import ai.lzy.v1.LzyPortalApi;
import ai.lzy.v1.Operations;
import io.grpc.stub.StreamObserver;
import org.junit.Assert;

import java.util.function.Consumer;

public class GrpcUtils {

    public static ChannelManager.ChannelCreateRequest makeCreateDirectChannelCommand(String workflowId,
        String channelName) {
        return ChannelManager.ChannelCreateRequest.newBuilder()
            .setWorkflowId(workflowId)
            .setChannelSpec(
                Channels.ChannelSpec.newBuilder()
                    .setChannelName(channelName)
                    .setContentType(makePlainTextDataScheme())
                    .setDirect(Channels.DirectChannelType.getDefaultInstance())
                    .build()
            ).build();
    }

    public static ChannelManager.ChannelDestroyRequest makeDestroyChannelCommand(String channelId) {
        return ChannelManager.ChannelDestroyRequest.newBuilder()
            .setChannelId(channelId)
            .build();
    }

    public static ChannelManager.ChannelDestroyAllRequest makeDestroyAllCommand(String workflowId) {
        return ChannelManager.ChannelDestroyAllRequest.newBuilder().setWorkflowId(workflowId).build();
    }

    public static Operations.DataScheme makePlainTextDataScheme() {
        return Operations.DataScheme.newBuilder()
            .setType("text")
            .setSchemeType(Operations.SchemeType.plain)
            .build();
    }

    public static Operations.Slot makeInputFileSlot(String slotName) {
        return Operations.Slot.newBuilder()
            .setName(slotName)
            .setMedia(Operations.Slot.Media.FILE)
            .setDirection(Operations.Slot.Direction.INPUT)
            .setContentType(makePlainTextDataScheme())
            .build();
    }

    public static Operations.Slot makeOutputFileSlot(String slotName) {
        return Operations.Slot.newBuilder()
            .setName(slotName)
            .setMedia(Operations.Slot.Media.FILE)
            .setDirection(Operations.Slot.Direction.OUTPUT)
            .setContentType(makePlainTextDataScheme())
            .build();
    }

    public static Operations.Slot makeInputPipeSlot(String slotName) {
        return Operations.Slot.newBuilder()
            .setName(slotName)
            .setMedia(Operations.Slot.Media.PIPE)
            .setDirection(Operations.Slot.Direction.INPUT)
            .setContentType(makePlainTextDataScheme())
            .build();
    }

    public static Operations.Slot makeOutputPipeSlot(String slotName) {
        return Operations.Slot.newBuilder()
            .setName(slotName)
            .setMedia(Operations.Slot.Media.PIPE)
            .setDirection(Operations.Slot.Direction.OUTPUT)
            .setContentType(makePlainTextDataScheme())
            .build();
    }

    public static LzyPortalApi.PortalSlotDesc.Ordinary makeAmazonSnapshot(String key, String bucket,
                                                                          String endpoint) {
        return LzyPortalApi.PortalSlotDesc.Ordinary.newBuilder()
            .setS3Coords(LzyPortalApi.S3Coords.newBuilder()
                .setKey(key)
                .setBucket(bucket)
                .setAmazon(LzyPortalApi.AmazonS3Endpoint.newBuilder()
                    .setAccessToken("")
                    .setSecretToken("")
                    .setEndpoint(endpoint)
                    .build()))
            .build();
    }

    public static LzyPortalApi.PortalSlotDesc.Ordinary makeLocalSnapshot(String snapshotId) {
        return LzyPortalApi.PortalSlotDesc.Ordinary.newBuilder()
            .setLocalId(snapshotId)
            .build();
    }

    public static LzyPortalApi.PortalSlotDesc.StdOut makeStdoutStorage(String taskId) {
        return LzyPortalApi.PortalSlotDesc.StdOut.newBuilder()
            .setTaskId(taskId)
            .build();
    }

    public static LzyPortalApi.PortalSlotDesc.StdErr makeStderrStorage(String taskId) {
        return LzyPortalApi.PortalSlotDesc.StdErr.newBuilder()
            .setTaskId(taskId)
            .build();
    }

    public static int rollPort() {
        return FreePortFinder.find(10000, 20000);
    }

    public abstract static class SuccessStreamObserver<T> implements StreamObserver<T> {

        @Override
        public void onError(Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.getMessage());
        }

        public static <T> SuccessStreamObserver<T> wrap(Consumer<T> onMessage) {
            return new SuccessStreamObserver<>() {
                @Override
                public void onNext(T value) {
                    onMessage.accept(value);
                }

                @Override
                public void onCompleted() {
                }
            };
        }

        public static <T> SuccessStreamObserver<T> wrap(Consumer<T> onMessage, Runnable onFinish) {
            return new SuccessStreamObserver<>() {
                @Override
                public void onNext(T value) {
                    onMessage.accept(value);
                }

                @Override
                public void onCompleted() {
                    onFinish.run();
                }
            };
        }
    }
}
