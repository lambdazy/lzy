package ru.yandex.qe.s3.amazon.transfer.loop;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.meta.MetadataBuilder;

import java.util.Map;

/**
 * Established by terry
 * on 18.01.16.
 */
public class AmazonMetadataConverter {

    public static ObjectMetadata from(Metadata metadata) {
        final ObjectMetadata amazonMetadata = new ObjectMetadata();
        for (Map.Entry<String, Object> entry : metadata.getMetadata().entrySet()) {
            amazonMetadata.setHeader(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Object> entry : metadata.getUserMetadata().entrySet()) {
            if (entry.getValue() instanceof String) {
                amazonMetadata.addUserMetadata(entry.getKey(), (String)entry.getValue());
            }//skip not string objects
        }
        if (metadata.getObjectContentLength() != Metadata.UNDEFINED_LENGTH) {
            amazonMetadata.setContentLength(metadata.getObjectContentLength());
        }
        return amazonMetadata;
    }

    public static Metadata to(ObjectMetadata amazonMetadata) {
        final MetadataBuilder metadataBuilder = new MetadataBuilder()
                .addMetadata(amazonMetadata.getRawMetadata());
        for (Map.Entry<String, String> entry : amazonMetadata.getUserMetadata().entrySet()) {
            metadataBuilder.addUserMetadata(entry.getKey(), entry.getValue());
        }

        final Long length = (Long)amazonMetadata.getRawMetadataValue(Headers.CONTENT_LENGTH);
        if (length != null) {
            metadataBuilder.setObjectContentLength(length);
        }
        return metadataBuilder.build();
    }
}
