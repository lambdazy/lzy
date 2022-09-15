package ai.lzy.test;

import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.common.LMD;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.portal.LzyPortal;
import io.grpc.stub.StreamObserver;
import org.junit.Assert;

import java.util.function.Consumer;

public class GrpcUtils {

    public static LCMS.ChannelCreateRequest makeCreateDirectChannelCommand(String workflowId, String channelName) {
        return LCMS.ChannelCreateRequest.newBuilder()
            .setWorkflowId(workflowId)
            .setChannelSpec(
                LCM.ChannelSpec.newBuilder()
                    .setChannelName(channelName)
                    .setContentType(makePlainTextDataScheme())
                    .setDirect(LCM.DirectChannelType.getDefaultInstance())
                    .build()
            ).build();
    }

    public static LCMS.ChannelDestroyRequest makeDestroyChannelCommand(String channelId) {
        return LCMS.ChannelDestroyRequest.newBuilder()
            .setChannelId(channelId)
            .build();
    }

    public static LCMS.ChannelDestroyAllRequest makeDestroyAllCommand(String workflowId) {
        return LCMS.ChannelDestroyAllRequest.newBuilder().setWorkflowId(workflowId).build();
    }

    public static LMD.DataScheme makePlainTextDataScheme() {
        return LMD.DataScheme.newBuilder()
            .setType("text")
            .setSchemeType(LMD.SchemeType.plain.name())
            .build();
    }

    public static LMS.Slot makeInputFileSlot(String slotName) {
        return LMS.Slot.newBuilder()
            .setName(slotName)
            .setMedia(LMS.Slot.Media.FILE)
            .setDirection(LMS.Slot.Direction.INPUT)
            .setContentType(makePlainTextDataScheme())
            .build();
    }

    public static LMS.Slot makeOutputFileSlot(String slotName) {
        return LMS.Slot.newBuilder()
            .setName(slotName)
            .setMedia(LMS.Slot.Media.FILE)
            .setDirection(LMS.Slot.Direction.OUTPUT)
            .setContentType(makePlainTextDataScheme())
            .build();
    }

    public static LMS.Slot makeInputPipeSlot(String slotName) {
        return LMS.Slot.newBuilder()
            .setName(slotName)
            .setMedia(LMS.Slot.Media.PIPE)
            .setDirection(LMS.Slot.Direction.INPUT)
            .setContentType(makePlainTextDataScheme())
            .build();
    }

    public static LMS.Slot makeOutputPipeSlot(String slotName) {
        return LMS.Slot.newBuilder()
            .setName(slotName)
            .setMedia(LMS.Slot.Media.PIPE)
            .setDirection(LMS.Slot.Direction.OUTPUT)
            .setContentType(makePlainTextDataScheme())
            .build();
    }

    public static LzyPortal.PortalSlotDesc.Snapshot makeAmazonSnapshot(String key, String bucket,
                                                                       String endpoint)
    {
        return LzyPortal.PortalSlotDesc.Snapshot.newBuilder()
            .setS3(LMS3.S3Locator.newBuilder()
                .setKey(key)
                .setBucket(bucket)
                .setAmazon(LMS3.AmazonS3Endpoint.newBuilder()
                    .setAccessToken("")
                    .setSecretToken("")
                    .setEndpoint(endpoint)
                    .build()))
            .build();
    }

    public static LzyPortal.PortalSlotDesc.StdOut makeStdoutStorage(String taskId) {
        return LzyPortal.PortalSlotDesc.StdOut.newBuilder()
            .setTaskId(taskId)
            .build();
    }

    public static LzyPortal.PortalSlotDesc.StdErr makeStderrStorage(String taskId) {
        return LzyPortal.PortalSlotDesc.StdErr.newBuilder()
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
