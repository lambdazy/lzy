package ai.lzy.portal.s3;

import com.amazonaws.services.s3.AmazonS3;
import ru.yandex.qe.s3.amazon.repository.AmazonS3Repository;
import ru.yandex.qe.s3.repository.BiDirectS3Converter;
import ru.yandex.qe.s3.transfer.Transmitter;

public class AmazonS3RepositoryAdapter<T> extends AmazonS3Repository<T> implements S3Repository<T> {

    public AmazonS3RepositoryAdapter(AmazonS3 amazonS3, Transmitter transmitter,
                                     int toStreamPoolSize, BiDirectS3Converter<T> converter)
    {
        super(amazonS3, transmitter, toStreamPoolSize, "", converter);
    }

    @Override
    public boolean contains(String bucket, String key) {
        return amazonS3.doesObjectExist(bucket, key);
    }
}
