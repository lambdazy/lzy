package ru.yandex.cloud.ml.platform.lzy.servant.snapshot.storage;

import ru.yandex.qe.s3.transfer.Transmitter;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

import javax.xml.bind.annotation.XmlType;
import java.net.URI;

public interface SnapshotStorage {
    Transmitter transmitter();
    URI getURI(String bucketName, String key);

    String DEFAULT_TRANSMITTER_NAME = "transmitter";
    int DEFAULT_DOWNLOAD_POOL_SIZE = 10;
    int DEFAULT_UPLOAD_POOL_SIZE = 10;

    static SnapshotStorage create(Lzy.GetS3CredentialsResponse credentials, String transmitterName, int downloadsPoolSize, int chunksPoolSize){
        if (credentials.hasAmazon()){
            return new AmazonSnapshotStorage(credentials.getAmazon(), transmitterName, downloadsPoolSize, chunksPoolSize);
        }
        else if (credentials.hasAzure()) {
            return new AzureSnapshotStorage(credentials.getAzure(), transmitterName, downloadsPoolSize, chunksPoolSize);
        }
        else {
            return new AzureSnapshotStorage(credentials.getAzureSas(), transmitterName, downloadsPoolSize, chunksPoolSize);
        }
    }
    static SnapshotStorage create(Lzy.GetS3CredentialsResponse credentials){
        return create(credentials, DEFAULT_TRANSMITTER_NAME, DEFAULT_DOWNLOAD_POOL_SIZE, DEFAULT_UPLOAD_POOL_SIZE);
    }
}
