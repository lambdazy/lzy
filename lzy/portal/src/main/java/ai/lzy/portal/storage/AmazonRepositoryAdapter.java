package ai.lzy.portal.storage;

import com.amazonaws.services.s3.AmazonS3;
import ru.yandex.qe.s3.amazon.repository.AmazonS3Repository;
import ru.yandex.qe.s3.repository.BiDirectS3Converter;
import ru.yandex.qe.s3.transfer.Transmitter;

import java.net.URI;

public class AmazonRepositoryAdapter<T> extends AmazonS3Repository<T> implements Repository<T> {

    public AmazonRepositoryAdapter(AmazonS3 amazonS3, Transmitter transmitter,
                                   int toStreamPoolSize, BiDirectS3Converter<T> converter)
    {
        super(amazonS3, transmitter, toStreamPoolSize, "", converter);
    }

    public boolean contains(URI uri) {
        return amazonS3.doesObjectExist(uri.getHost(), Utils.removeLeadingSlash(uri.getPath()));
    }

    @Override
    public void put(URI uri, T value) {
        super.put(uri.getHost(), Utils.removeLeadingSlash(uri.getPath()), value);
    }

    @Override
    public T get(URI uri) {
        return super.get(uri.getHost(), Utils.removeLeadingSlash(uri.getPath()));
    }

    @Override
    public void remove(URI uri) {
        super.remove(uri.getHost(), Utils.removeLeadingSlash(uri.getPath()));
    }
}
