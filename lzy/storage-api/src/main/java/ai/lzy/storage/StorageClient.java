package ai.lzy.storage;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

/**
 * Blocking client to user's data storage (e.g., AmazonS3 or AzureBlobsStorage).
 */
public interface StorageClient {
    void read(URI uri, Path destination) throws InterruptedException, IOException;
    void write(URI uri, Path source) throws InterruptedException, IOException;
    boolean blobExists(URI uri) throws IOException;
}
