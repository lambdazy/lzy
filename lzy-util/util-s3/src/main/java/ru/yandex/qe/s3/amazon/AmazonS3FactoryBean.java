package ru.yandex.qe.s3.amazon;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

/**
 * Established by terry on 01.07.15.
 */
public class AmazonS3FactoryBean implements FactoryBean<AmazonS3> {

    private String endpoint;
    private String accessKey;
    private String secretKey;
    private boolean forceV2Signer = true;

    private ClientConfiguration configuration;

    @Required
    public void setEndpoint(@Nonnull String endpoint) {
        this.endpoint = endpoint;
    }

    @Required
    public void setAccessKey(@Nonnull String accessKey) {
        this.accessKey = accessKey;
    }

    @Required
    public void setSecretKey(@Nonnull String secretKey) {
        this.secretKey = secretKey;
    }

    @Required
    public void setConfiguration(@Nonnull ClientConfiguration configuration) {
        this.configuration = configuration;
    }

    public void setForceV2Signer(boolean forceV2Signer) {
        this.forceV2Signer = forceV2Signer;
    }

    @Override
    public AmazonS3 getObject() {
        final ClientConfiguration configuration = new ClientConfiguration(this.configuration);
        if (forceV2Signer) {
            // Force V2 signer for compatibility with CEPH's radosgw and other "S3-like" services.
            // Otherwise, the AWS SDK will use V4 signer for all endpoints which it doesn't know.
            // @see https://github.com/aws/aws-sdk-java/issues/277#issuecomment-58129144
            configuration.setSignerOverride("S3SignerType");
        }

        AmazonS3 amazonS3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), configuration);
        amazonS3Client.setEndpoint(endpoint);
        return amazonS3Client;
    }

    @Override
    public Class<?> getObjectType() {
        return AmazonS3.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
