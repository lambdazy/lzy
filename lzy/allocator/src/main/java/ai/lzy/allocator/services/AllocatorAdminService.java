package ai.lzy.allocator.services;

import ai.lzy.allocator.admin.ImagesUpdater;
import ai.lzy.allocator.admin.dao.AdminDao;
import ai.lzy.allocator.model.ActiveImages.JupyterLabImage;
import ai.lzy.allocator.model.ActiveImages.SyncImage;
import ai.lzy.allocator.model.ActiveImages.WorkerImage;
import ai.lzy.v1.AllocatorAdminGrpc;
import ai.lzy.v1.VmAllocatorAdminApi;
import ai.lzy.v1.VmAllocatorAdminApi.ActiveImages;
import ai.lzy.v1.VmAllocatorAdminApi.JupyterLabImages;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Optional;

import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class AllocatorAdminService extends AllocatorAdminGrpc.AllocatorAdminImplBase {
    private static final Logger LOG = LogManager.getLogger(AllocatorAdminService.class);

    private final AdminDao adminDao;
    @Nullable
    private final ImagesUpdater imagesUpdater;

    @Inject
    public AllocatorAdminService(AdminDao adminDao, Optional<ImagesUpdater> imagesUpdater) {
        this.adminDao = adminDao;
        this.imagesUpdater = imagesUpdater.orElse(null);
    }

    @Override
    public void setWorkerImages(VmAllocatorAdminApi.WorkerImages request, StreamObserver<ActiveImages> response) {
        var workers = request.getImagesList().stream()
            .map(WorkerImage::of)
            .toList();

        LOG.info("About to set new worker images: {}", request);

        try {
            withRetries(LOG, () -> adminDao.setWorkerImages(workers));
        } catch (Exception e) {
            LOG.error("Cannot set workers images", e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        reply(response);
    }

    @Override
    public void setSyncImage(VmAllocatorAdminApi.SyncImage request, StreamObserver<ActiveImages> response) {
        LOG.info("About to set new sync image: {}", request);

        try {
            withRetries(LOG, () -> adminDao.setSyncImage(SyncImage.of(request.getImage())));
        } catch (Exception e) {
            LOG.error("Cannot set sync image", e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        reply(response);
    }

    @Override
    public void setJupyterlabImages(JupyterLabImages request, StreamObserver<ActiveImages> response) {
        LOG.info("About to set new jupyter lab images: {}", request);

        var jls = request.getImagesList().stream()
            .map(x -> JupyterLabImage.of(x.getMainImage(), x.getAdditionalImagesList().toArray(new String[0])))
            .toList();

        try {
            withRetries(LOG, () -> adminDao.setJupyterLabImages(jls));
        } catch (Exception e) {
            LOG.error("Cannot set jupyter lab images", e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        reply(response);
    }

    @Override
    public void getActiveImages(Empty request, StreamObserver<ActiveImages> response) {
        reply(response);
    }

    @Override
    public void updateImages(Empty request, StreamObserver<ActiveImages> response) {
        ai.lzy.allocator.model.ActiveImages.Configuration conf;
        try {
            conf = withRetries(LOG, adminDao::getImages);
        } catch (Exception e) {
            LOG.error("Cannot load active configuration", e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        if (conf.isEmpty()) {
            LOG.error("Cannot update images: {}", conf);
            response.onError(Status.FAILED_PRECONDITION.withDescription("Active configuration is empty").asException());
            return;
        }

        LOG.info("Attempt to update active images to: {}", conf);

        try {
            imagesUpdater.update(conf);
            reply(response, toProto(conf));
        } catch (ImagesUpdater.UpdateDaemonSetsException e) {
            LOG.error("Cannot update images: {}", e.getMessage());
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    private void reply(StreamObserver<ActiveImages> response) {
        ActiveImages conf;
        try {
            var images = withRetries(LOG, adminDao::getImages);
            conf = toProto(images);
        } catch (Exception e) {
            LOG.error("Cannot load active images", e);
            response.onError(Status.UNAVAILABLE.withDescription("Cannot load active images").asException());
            return;
        }

        reply(response, conf);
    }

    private static void reply(StreamObserver<ActiveImages> response, ActiveImages conf) {
        LOG.info("ActiveImages: {}", conf);
        response.onNext(conf);
        response.onCompleted();
    }

    private static ActiveImages toProto(ai.lzy.allocator.model.ActiveImages.Configuration conf) {
        var workers = VmAllocatorAdminApi.WorkerImages.newBuilder();
        for (var worker : conf.workers()) {
            workers.addImages(worker.image());
        }

        var sync = VmAllocatorAdminApi.SyncImage.newBuilder()
            .setImage(Optional.ofNullable(conf.sync().image()).orElse(""))
            .build();

        var jls = JupyterLabImages.newBuilder();
        for (var jl : conf.jupyterLabs()) {
            jls.addImages(JupyterLabImages.Images.newBuilder()
                .setMainImage(jl.mainImage())
                .addAllAdditionalImages(Arrays.asList(jl.additionalImages()))
                .build());
        }

        return ActiveImages.newBuilder()
            .setWorker(workers.build())
            .setSync(sync)
            .setJl(jls.build())
            .build();
    }
}
