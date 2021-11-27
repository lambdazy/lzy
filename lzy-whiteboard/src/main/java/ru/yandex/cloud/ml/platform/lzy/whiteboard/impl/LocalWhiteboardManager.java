package ru.yandex.cloud.ml.platform.lzy.whiteboard.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.WhiteboardManager;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

import java.net.URI;
import java.util.ArrayList;
import java.util.UUID;

public class LocalWhiteboardManager extends WhiteboardManager {
    private static final Logger LOG = LogManager.getLogger(LocalWhiteboardManager.class);
    @Override
    public void prepareToSaveData(UUID wbId, String operationName, Slot slot, URI uri) {
        LOG.info("LocalWhiteboardManager::prepareToSaveData invoked with whiteboardId=" + wbId + ", " +
                "with operationName=" + operationName + ", with slot=" + slot.name() + ", with URI=" + uri);
    }

    @Override
    public void commit(UUID wbId, String operationName, Slot slot) {
        LOG.info("LocalWhiteboardManager::commit invoked with whiteboardId=" + wbId + ", " +
                "with operationName=" + operationName + ", with slot=" + slot.name());
    }

    @Override
    public void addDependency(UUID wbId, String from, String to) {
        LOG.info("LocalWhiteboardManager::addDependency invoked with whiteboardId=" + wbId + ", " +
                "with operationFrom=" + from + ", with operationTo=" + to);
    }

    @Override
    public void addDependencies(UUID wbId, ArrayList<String> from, String to) {
        LOG.info("LocalWhiteboardManager::addDependencies invoked with whiteboardId=" + wbId + ", " +
                ", with operationTo=" + to);
    }

    @Override
    public void getWhiteboardById(UUID wbId, IAM.UserCredentials auth) {
        LOG.info("LocalWhiteboardManager::getWhiteboardById invoked with whiteboardId=" + wbId);
    }
}
