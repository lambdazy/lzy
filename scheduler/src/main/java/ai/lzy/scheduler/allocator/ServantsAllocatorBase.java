package ai.lzy.scheduler.allocator;

import ai.lzy.scheduler.db.DaoException;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.servant.Servant;
import io.grpc.Status;
import io.grpc.StatusException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ServantsAllocatorBase implements ServantsAllocator {
    protected final ServantDao dao;
    private final Map<ServantKey, AllocateResult> requests = new ConcurrentHashMap<>();
    private static final Logger LOG = LogManager.getLogger(ServantsAllocatorBase.class);

    public ServantsAllocatorBase(ServantDao dao) {
        this.dao = dao;
    }

    protected void saveRequest(String workflowId, String servantId, String token, String meta){
        requests.put(new ServantKey(workflowId, servantId), new AllocateResult(token, meta));
    }

    @Nullable
    protected AllocateResult getRequest(String workflowId, String servantId) {
        var res = requests.get(new ServantKey(workflowId, servantId));
        if (res != null) {
            return res;
        }
        try {
            var servant = dao.get(workflowId, servantId);
            if (servant == null) {
                return null;
            }
            return new AllocateResult(servant.allocationToken(), servant.allocatorMetadata());
        } catch (DaoException e) {
            LOG.error("Cannot get data from dao", e);
            return null;
        }
    }

    @Override
    public void register(String workflowId, String servantId,
                         URI servantUri, String servantToken) throws StatusException {

        var request = getRequest(workflowId, servantId);
        if (request == null) {
            throw Status.INVALID_ARGUMENT.withDescription("Cannot get request").asException();
        }

        if (servantToken == null || !Objects.equals(request.allocationToken(), servantToken)) {
            LOG.error("Wrong allocation token from servant <{}> in workflow <{}>", servantId, workflowId);
            throw Status.NOT_FOUND.withDescription("Servant not found in workflow").asException();
        }

        final Servant servant;
        try {
            servant = dao.get(workflowId, servantId);
        } catch (DaoException e) {
            throw Status.INTERNAL.asException();
        }

        if (servant == null) {
            throw Status.NOT_FOUND.withDescription("Servant not found in workflow").asException();
        }
        try {
            servant.notifyConnected(servantUri.toURL());
        } catch (MalformedURLException e) {
            throw Status.INVALID_ARGUMENT.withDescription("Invalid url").asException();
        }
    }

    private record ServantKey(String workflowId, String servantId) {}
}
