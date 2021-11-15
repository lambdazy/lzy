package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;

public class LocalStorageManager implements StorageManager {
    @Override
    public String getStorageUri(String slotName, String taskId) {
        return "Link to the storage";
    }

    @Override
    public void prepareToSaveData(String slotName, String taskId) {
        // getStorageUri
        // create entry in database
        // set status to IN_PROGRESS
    }

    @Override
    public void saveDataReceiver(ByteString chunk, String slotName, String taskId) {
        // getStorageUri
        // save chunk
    }

    @Override
    public void saveDataSender(ByteString chunk, String slotName, String taskId) {
        // getStorageUri
        // save chunk
    }

    @Override
    public void end(String slotName, String taskId) {
        // set status to FINISHED
    }
}
