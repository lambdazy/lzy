package ai.lzy.iam.grpc.ycclient;

import ai.lzy.iam.clients.AccessClient;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.impl.Root;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.YcSubject;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthInvalidArgumentException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.auth.api.CloudAuthClient;
import yandex.cloud.auth.api.Resource;
import yandex.cloud.auth.api.exception.CloudAuthException;

import java.util.function.Supplier;

public class YcAccessServiceGrpcClient implements AccessClient {
    private static final Logger LOG = LogManager.getLogger(YcAccessServiceGrpcClient.class);
    private static final String YC_PERMISSION_FOLDER_UPDATE = "resource-manager.folders.update";

    private final CloudAuthClient authClient;

    public YcAccessServiceGrpcClient(CloudAuthClient authClient) {
        this.authClient = authClient;
    }

    @Override
    public AccessClient withToken(Supplier<Credentials> tokenSupplier) {
        return this;
    }

    @Override
    public boolean hasResourcePermission(Subject subject, AuthResource resourceId, AuthPermission permission)
        throws AuthException
    {
        if (!(subject instanceof YcSubject ycSubject)) {
            LOG.error("Unexpected subject {}", subject);
            throw new AuthInvalidArgumentException("Unexpected subject " + subject);
        }

        // authorize only lzy-internal-user

        if (!(ycSubject.provided() instanceof yandex.cloud.auth.api.Subject.ServiceAccount sa)) {
            LOG.error("Unexpected subject {}", subject);
            throw new AuthInvalidArgumentException("Unexpected subject " + subject);
        }

        if (resourceId != Root.INSTANCE || permission != AuthPermission.INTERNAL_AUTHORIZE) {
            LOG.error("Unexpected auth resource/permission {}/{}", resourceId, permission);
            throw new AuthInvalidArgumentException(
                "Unexpected auth resource/permission: %s/%s".formatted(resourceId, permission));
        }

        LOG.debug("Authorizing SA {} in folder {} to {}",
            sa.toId().getId(), sa.getFolderId(), YC_PERMISSION_FOLDER_UPDATE);

        try {
            authClient.authorize(sa.toId(), YC_PERMISSION_FOLDER_UPDATE, Resource.folder(sa.getFolderId()));
            return true;
        } catch (CloudAuthException e) {
            LOG.warn("Not authorized: {}", e.getInternalDetails());
            return false;
        }
    }
}
