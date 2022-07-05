package ai.lzy.scheduler.allocator.impl;

import ai.lzy.scheduler.allocator.ServantMetaStorage;
import ai.lzy.scheduler.db.ServantMetaStorageDao;
import ai.lzy.scheduler.db.ServantMetaStorageDao.MetaStorageEntry;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.UUID;

@Singleton
public class ServantMetaStorageImpl implements ServantMetaStorage {
    private final ServantMetaStorageDao dao;

    @Inject
    public ServantMetaStorageImpl(ServantMetaStorageDao dao) {
        this.dao = dao;
    }

    @Override
    public String generateToken(String workflowId, String servantId) {
        final MetaStorageEntry entry = dao.get(workflowId, servantId);
        final String token = UUID.randomUUID().toString();
        if (entry != null) {
            if (entry.token() != null) {
                throw new IllegalStateException("Token already exists");
            }
            dao.save(new MetaStorageEntry(workflowId, servantId, entry.meta(), token));
            return token;
        }
        dao.save(new MetaStorageEntry(workflowId, servantId, null, token));
        return token;
    }

    @Override
    public void clear(String workflowId, String servantId) {
        dao.remove(workflowId, servantId);
    }

    @Override
    public void saveMeta(String workflowId, String servantId, String meta) {
        final MetaStorageEntry entry = dao.get(workflowId, servantId);
        if (entry == null) {
            dao.save(new MetaStorageEntry(workflowId, servantId, meta, null));
            return;
        }
        dao.save(new MetaStorageEntry(workflowId, servantId, meta, entry.token()));
    }

    @Override
    public String getMeta(String workflowId, String servantId) {
        final MetaStorageEntry entry = dao.get(workflowId, servantId);
        if (entry == null) {
            return null;
        }
        return entry.meta();
    }

    @Override
    public boolean auth(String workflowId, String servantId, String token) {
        final MetaStorageEntry entry = dao.get(workflowId, servantId);
        if (entry == null) {
            return false;
        }
        final var actualToken = entry.token();
        return actualToken != null && actualToken.equals(token);
    }
}
