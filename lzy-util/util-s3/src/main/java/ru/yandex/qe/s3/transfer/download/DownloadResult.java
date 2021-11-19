package ru.yandex.qe.s3.transfer.download;

/**
 * Established by terry
 * on 16.07.15.
 */
public class DownloadResult<T> {

    private final DownloadState downloadState;
    private final T processingResult;

    public DownloadResult(DownloadState downloadState, T processingResult) {
        this.downloadState = downloadState;
        this.processingResult = processingResult;
    }

    public DownloadState getDownloadState() {
        return downloadState;
    }

    public T getProcessingResult() {
        return processingResult;
    }
}
