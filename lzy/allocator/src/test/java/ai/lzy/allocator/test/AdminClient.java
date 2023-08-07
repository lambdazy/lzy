//package ai.lzy.allocator.test;
//
//import ai.lzy.util.auth.credentials.RenewableJwt;
//import ai.lzy.v1.AllocatorAdminGrpc;
//import ai.lzy.v1.VmAllocatorAdminApi;
//import com.google.protobuf.Empty;
//
//import java.nio.file.Path;
//import java.time.Duration;
//import java.util.List;
//
//import static ai.lzy.util.auth.credentials.CredentialsUtils.readPrivateKey;
//import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
//import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
//
//public class AdminClient {
//
//    private static final String SYNC_IMAGE = "lzydock/kuber-node-sync:1.0";
//    private static final String WORKER_IMAGE = "cr.yandex/crp1pj4c7m19ncjiff3f/worker:impliment-net-policy-1.3";
//
//    public static void main(String[] args) throws Exception {
//        var jwt = new RenewableJwt("lzy-admin", "INTERNAL", Duration.ofDays(1),
//            readPrivateKey(Path.of("admin-user-private.pem")));
//        var channel = newGrpcChannel("localhost:10239", AllocatorAdminGrpc.SERVICE_NAME);
//        var client = newBlockingClient(AllocatorAdminGrpc.newBlockingStub(channel), "admin", () -> jwt.get().token());
//
//        var resp = client.getActiveImages(Empty.getDefaultInstance());
//        printActiveConfiguration(resp);
//
//        setSyncImage(client, SYNC_IMAGE);
//        setWorkerImage(client, WORKER_IMAGE);
//        //setJupyterLabImage(client, "", List.of(""));
//
//        System.out.println("Update images...");
//        resp = client.updateImages(Empty.getDefaultInstance());
//        printActiveConfiguration(resp);
//
//        channel.shutdown();
//    }
//
//    private static void setSyncImage(AllocatorAdminGrpc.AllocatorAdminBlockingStub client, String image) {
//        System.out.println("Set SyncImage to " + image);
//        var resp = client.setSyncImage(VmAllocatorAdminApi.SyncImage.newBuilder()
//            .setImage(image)
//            .build());
//        printActiveConfiguration(resp);
//    }
//
//    private static void setWorkerImage(AllocatorAdminGrpc.AllocatorAdminBlockingStub client, String image) {
//        System.out.println("Set WorkerImage to " + image);
//        var resp = client.setWorkerImages(VmAllocatorAdminApi.WorkerImages.newBuilder()
//            .addImages(image)
//            .build());
//        printActiveConfiguration(resp);
//    }
//
//    private static void setJupyterLabImage(AllocatorAdminGrpc.AllocatorAdminBlockingStub client, String dindImage,
//                                           List<String> internalImages)
//    {
//        System.out.printf("""
//            Set JupyterLabImages to:
//              main dind image: %s,
//              other images   : %s
//            %n""", dindImage, String.join(", ", internalImages));
//        var resp = client.setJupyterlabImages(VmAllocatorAdminApi.JupyterLabImages.newBuilder()
//            .addImages(VmAllocatorAdminApi.JupyterLabImages.Images.newBuilder()
//                .setMainImage(dindImage)
//                .addAllAdditionalImages(internalImages)
//                .build())
//            .build());
//        printActiveConfiguration(resp);
//    }
//
//    private static void printActiveConfiguration(VmAllocatorAdminApi.ActiveImages resp) {
//        System.out.println("Active Configuration: \n" + resp + "\n");
//    }
//}
