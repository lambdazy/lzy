package ai.lzy.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;

/**
 * Blocking client to user's data storage (e.g., AmazonS3 or AzureBlobsStorage).
 */
public interface StorageClient {
    void read(URI uri, Path destination) throws InterruptedException, IOException;
    void read(URI uri, OutputStream destination) throws InterruptedException, IOException;
    void write(URI uri, Path source) throws InterruptedException, IOException;
    void write(URI uri, InputStream source) throws InterruptedException, IOException;
    boolean blobExists(URI uri) throws IOException;
}
