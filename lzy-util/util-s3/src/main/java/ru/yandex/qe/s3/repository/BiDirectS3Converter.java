package ru.yandex.qe.s3.repository;

import javax.annotation.WillClose;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Established by terry
 * on 14.07.15.
 */
public interface BiDirectS3Converter<T> {

    @WillClose
    void toStream(T value, OutputStream outputStream) throws IOException;

    @WillClose
    T fromStream(InputStream inputStream) throws IOException;
}
