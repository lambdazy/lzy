package ai.lzy.disk.s3;

import ai.lzy.disk.configs.S3Config;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Factory
public class S3ClientFactory {

    @Singleton
    @Requires(property = "disk-service.amazonS3.accessKey")
    @Requires(property = "disk-service.amazonS3.secretKey")
    @Requires(property = "disk-service.amazonS3.endpoint")
    @Requires(property = "disk-service.amazonS3.region")
    public AmazonS3 amazonS3(S3Config s3Config) {
        final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                s3Config.accessKey(),
                s3Config.secretKey()
            )))
            .withEndpointConfiguration(new AmazonS3ClientBuilder.EndpointConfiguration(
                s3Config.endpoint(),
                s3Config.region()
            ))
            .withPathStyleAccessEnabled(true)
            .build();
        return s3Client;
    }

}
