package ru.yandex.qe.s3.transfer.buffers;

/**
 * Established by terry
 * on 14.07.15.
 */
public enum ByteBufferSizeType {

    _8_MB(8), _16_MB(16), _32_MB(32), _64_MB(64), _128_MB(128), _256_MB(256), _512_MB(512);

    private int sizeInBytes;

    ByteBufferSizeType(int mbSize) {
        this.sizeInBytes = mbSize * 1024 * 1024;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }
}
