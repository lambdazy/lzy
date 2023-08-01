package ai.lzy.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.download.DownloadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

public abstract class StorageClientWithTransmitter implements StorageClient {
    protected static final Logger LOG = LogManager.getLogger(StorageClientWithTransmitter.class);

    protected static final String DEFAULT_TRANSMITTER_NAME = "transmitter";

    protected abstract Transmitter transmitter();

    protected abstract DownloadRequest downloadRequest(URI uri);

    protected abstract UploadRequest uploadRequest(URI uri, InputStream source);

    @Override
    public void read(URI uri, Path destination) throws InterruptedException, IOException {
        LOG.info("Download data from '%s' to '%s'".formatted(uri.toString(), destination.toAbsolutePath().toString()));
        read(uri, new FileOutputStream(destination.toFile()));
    }

    @Override
    public void read(URI uri, OutputStream destination) throws InterruptedException, IOException {

        var downloadRequest = downloadRequest(uri);
        var future = transmitter().downloadC(downloadRequest, data -> {
            try (var source = new BufferedInputStream(data.getInputStream())) {
                source.transferTo(destination);
            }
        });
        try {
            future.get();
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }
    }

    @Override
    public void write(URI uri, Path source) throws InterruptedException, IOException {
        write(uri, new FileInputStream(source.toFile()));
    }

    @Override
    public void write(URI uri, InputStream source) throws InterruptedException, IOException {
        LOG.info("Upload data from to '%s'".formatted(uri.toString()));

        var uploadRequest = uploadRequest(uri, source);
        var future = transmitter().upload(uploadRequest);
        try {
            future.get();
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }

        LOG.info("Data to '%s' was loaded? -- %s".formatted(uri.toString(), blobExists(uri)));
    }
}
