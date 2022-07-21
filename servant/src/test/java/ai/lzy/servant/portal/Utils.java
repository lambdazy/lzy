package ai.lzy.servant.portal;

import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.v1.Channels;
import ai.lzy.v1.LzyPortalApi;
import ai.lzy.v1.Operations;
import io.grpc.stub.StreamObserver;
import org.junit.Assert;

import java.util.function.Consumer;

public class Utils {

    public static Channels.ChannelCommand makeCreateDirectChannelCommand(String channelName) {
        return Channels.ChannelCommand.newBuilder()
            .setChannelName(channelName)
            .setCreate(Channels.ChannelCreate.newBuilder()
                .setContentType(makePlainTextDataScheme())
                .setDirect(Channels.DirectChannelSpec.getDefaultInstance())
                .build())
            .build();
    }

    public static Channels.ChannelCommand makeDestroyChannelCommand(String channelName) {
        return Channels.ChannelCommand.newBuilder()
            .setChannelName(channelName)
            .setDestroy(Channels.ChannelDestroy.getDefaultInstance())
            .build();
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

    public static LzyPortalApi.PortalSlotDesc.Snapshot makeSnapshotStorage(String snapshotId) {
        return LzyPortalApi.PortalSlotDesc.Snapshot.newBuilder()
            .setId(snapshotId)
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

    abstract static class SuccessStreamObserver<T> implements StreamObserver<T> {
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
