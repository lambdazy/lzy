package ru.yandex.qe.s3.amazon;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.Nonnull;

/**
 * S3 access to MDS
 *
 * Before using please ensure you have successfully run S3MdsIT test with your configuration settings
 *
 * @author cherolex
 */
public class MdsAmazonS3FactoryBean implements FactoryBean<AmazonS3> {
  private String endpoint;
  private ClientConfiguration configuration;
  private AWSCredentialsProvider mdsCredentialsProvider;
  private String signingRegion = Regions.DEFAULT_REGION.getName();

  @Override
  public AmazonS3 getObject() {
    return AmazonS3ClientBuilder
            .standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, signingRegion))
            .withClientConfiguration(configuration)
            .withChunkedEncodingDisabled(true)
            .withPayloadSigningEnabled(false)
            .withCredentials(mdsCredentialsProvider)
            .build();
  }

  @Override
  public Class<?> getObjectType() {
    return AmazonS3.class;
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

  @Required
  public void setMdsCredentialsProvider(AWSCredentialsProvider mdsCredentialsProvider) {
    this.mdsCredentialsProvider = mdsCredentialsProvider;
  }

  @Required
  public void setEndpoint(@Nonnull String endpoint) {
    this.endpoint = endpoint;
  }

  @Required
  public void setConfiguration(@Nonnull ClientConfiguration configuration) {
    this.configuration = configuration;
  }

  public void setSigningRegion(@Nonnull String signingRegion) {
    this.signingRegion = signingRegion;
  }
}
