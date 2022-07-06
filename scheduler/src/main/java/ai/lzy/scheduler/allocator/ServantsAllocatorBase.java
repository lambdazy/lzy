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
}
