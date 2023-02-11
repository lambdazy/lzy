package ai.lzy.storage;

import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.download.DownloadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ExecutionException;

public abstract class StorageClientWithTransmitter implements StorageClient {
    protected static final String DEFAULT_TRANSMITTER_NAME = "transmitter";
    protected static final int DEFAULT_BYTE_BUFFER_SIZE = 10;

    protected abstract Transmitter transmitter();

    protected abstract DownloadRequest downloadRequest(URI uri);

    protected abstract UploadRequest uploadRequest(URI uri, Path source);

    @Override
    public void read(URI uri, Path destination) throws InterruptedException, IOException {
        var downloadRequest = downloadRequest(uri);
        var future = transmitter().downloadC(downloadRequest, data -> {
            try (var source = new BufferedInputStream(data.getInputStream())) {
                Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
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
        var uploadRequest = uploadRequest(uri, source);
        var future = transmitter().upload(uploadRequest);
        try {
            future.get();
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }
    }
}
