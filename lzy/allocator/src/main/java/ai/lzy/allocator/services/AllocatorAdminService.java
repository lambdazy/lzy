package ai.lzy.allocator.services;

import ai.lzy.allocator.admin.ImagesUpdater;
import ai.lzy.allocator.admin.dao.AdminDao;
import ai.lzy.v1.AllocatorAdminGrpc;
import ai.lzy.v1.VmAllocatorAdminApi;
import ai.lzy.v1.VmAllocatorAdminApi.ActiveImages;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    public void setImages(VmAllocatorAdminApi.SetImagesRequest request, StreamObserver<ActiveImages> response) {
        var workers = request.getConfigsList().stream()
            .map(pc ->
                ai.lzy.allocator.model.ActiveImages.PoolConfig.of(
                    pc.getImagesList().stream().map(ai.lzy.allocator.model.ActiveImages.Image::of).toList(),
                    ai.lzy.allocator.model.ActiveImages.DindImages.of(
                        pc.getDindImages().getDindImage(),
                        pc.getDindImages().getAdditionalImagesList().stream().toList()
                    ),
                    pc.getPoolKind(),
                    pc.getPoolName()
                )
            )
            .toList();

        LOG.info("About to set new worker images: {}", request);

        try {
            withRetries(LOG, () -> adminDao.setImages(workers));
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
            withRetries(LOG, () -> adminDao.setSyncImage(ai.lzy.allocator.model.ActiveImages.Image.of(request.getImage())));
        } catch (Exception e) {
            LOG.error("Cannot set sync image", e);
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
        var builder = ActiveImages.newBuilder();
        for (var pc : conf.configs()) {
            var poolConfig = VmAllocatorAdminApi.PoolConfig.newBuilder();
            poolConfig.addAllImages(pc.images().stream().map(ai.lzy.allocator.model.ActiveImages.Image::image).toList());
            if (pc.dindImages() != null) {
                poolConfig.setDindImages(VmAllocatorAdminApi.DindImages.newBuilder()
                    .setDindImage(pc.dindImages().dindImage())
                        .addAllAdditionalImages(pc.dindImages().additionalImages())
                    .build());
            }
            poolConfig.setPoolKind(pc.kind());
            poolConfig.setPoolName(pc.name());
            builder.addConfig(poolConfig.build());
        }

        var sync = VmAllocatorAdminApi.SyncImage.newBuilder()
            .setImage(Optional.ofNullable(conf.sync().image()).orElse(""))
            .build();

        return builder
            .setSync(sync)
            .build();
    }
}
