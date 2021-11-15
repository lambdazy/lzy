package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;

// Think about concurrency
public interface StorageManager {
    enum status {
        IN_PROGRESS,
        FINISHED
    }

    String getStorageUri(String slotName, String taskId);

    // create entry in the database and set status IN_PROGRESS
    void prepareToSaveData(String slotName, String taskId);

    // save to persistent storage receiver slot data
    void saveDataReceiver(ByteString chunk, String slotName, String taskId);

    // save to persistent storage sender slot data
    void saveDataSender(ByteString chunk, String slotName, String taskId);

    // set status FINISHED
    void end(String slotName, String taskId);
}
