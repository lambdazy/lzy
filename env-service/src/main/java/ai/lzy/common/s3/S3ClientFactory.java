package ai.lzy.common.s3;

import ai.lzy.configs.DbConfig;
import ai.lzy.configs.S3Config;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Factory
public class S3ClientFactory {

    @Singleton
    @Requires(property = "s3.accessKey")
    @Requires(property = "s3.secretKey")
    @Requires(property = "s3.endpoint")
    @Requires(property = "s3.region")
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
