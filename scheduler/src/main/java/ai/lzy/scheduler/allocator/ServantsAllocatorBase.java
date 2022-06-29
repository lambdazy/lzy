package ai.lzy.scheduler.allocator;

import ai.lzy.scheduler.db.DaoException;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.servant.Servant;
import io.grpc.Status;
import io.grpc.StatusException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.Objects;

public abstract class ServantsAllocatorBase implements ServantsAllocator {
    protected final ServantDao dao;
    private static final Logger LOG = LogManager.getLogger(ServantsAllocatorBase.class);

    public ServantsAllocatorBase(ServantDao dao) {
        this.dao = dao;
    }

    @Override
    public void register(String workflowId, String servantId,
                         URI servantUri, String servantToken) throws StatusException {
        Servant servant;
        try {
            servant = dao.get(workflowId, servantId);
        } catch (DaoException e) {
            LOG.error(e);
            throw Status.INTERNAL.asException();
        }
        if (servant == null) {
            throw Status.INVALID_ARGUMENT.withDescription("Servant does not exists").asException();
        }

        if (servantToken == null || !Objects.equals(servant.allocationToken(), servantToken)) {
            LOG.error("Wrong allocation token from servant <{}> in workflow <{}>", servantId, workflowId);
            throw Status.NOT_FOUND.withDescription("Servant not found in workflow").asException();
        }

        try {
            servant.notifyConnected(servantUri.toURL());
        } catch (MalformedURLException e) {
            throw Status.INVALID_ARGUMENT.withDescription("Invalid url").asException();
        }
    }
}
