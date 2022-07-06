package ai.lzy.scheduler.allocator;

import ai.lzy.scheduler.db.DaoException;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.servant.Servant;
import io.grpc.Status;
import io.grpc.StatusException;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class ServantsAllocatorBase implements ServantsAllocator {
    private static final Logger LOG = LogManager.getLogger(ServantsAllocatorBase.class);

    protected final ServantDao dao;
    protected final ServantMetaStorage metaStorage;

    protected ServantsAllocatorBase(ServantDao dao, ServantMetaStorage metaStorage) {
        this.dao = dao;
        this.metaStorage = metaStorage;
    }

    @Override
    public void register(String workflowId, String servantId,
                         HostAndPort servantUri) throws StatusException {
        final Servant servant;
        try {
            servant = dao.get(workflowId, servantId);
        } catch (DaoException e) {
            throw Status.INTERNAL.asException();
        }

        if (servant == null) {
            throw Status.NOT_FOUND.withDescription("Servant not found in workflow").asException();
        }
        servant.notifyConnected(servantUri);
    }
}
