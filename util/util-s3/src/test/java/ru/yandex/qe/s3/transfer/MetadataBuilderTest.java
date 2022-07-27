package ru.yandex.qe.s3.transfer;

import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.meta.MetadataBuilder;

/**
 * Established by terry on 19.01.16.
 */
public class MetadataBuilderTest {

    @Test
    public void create_empty() {
        final Metadata metadata = new MetadataBuilder().build();
        Assert.assertTrue(metadata.getMetadata().isEmpty());
        Assert.assertTrue(metadata.getUserMetadata().isEmpty());
        Assert.assertTrue(metadata.getAclObjects().isEmpty());
        Assert.assertEquals(metadata.getObjectContentLength(), Metadata.UNDEFINED_LENGTH);
    }

    @Test
    public void add_meta_and_acl() {
        final Metadata metadata = new MetadataBuilder().addMetadata("key", "value")
            .addMetadata(Collections.singletonMap("key2", "value2"))
            .addUserMetadata("key3", "value")
            .addUserMetadata(Collections.singletonMap("key4", "value2"))
            .addAclObject("acl1")
            .addAclObjects(Collections.singletonList("acl2"))
            .setObjectContentLength(10).build();

        Assert.assertEquals(metadata.getMetadata().size(), 2);
        Assert.assertTrue(metadata.getMetadata().containsKey("key"));
        Assert.assertTrue(metadata.getMetadata().containsKey("key2"));
        Assert.assertEquals(metadata.getUserMetadata().size(), 2);
        Assert.assertTrue(metadata.getUserMetadata().containsKey("key3"));
        Assert.assertTrue(metadata.getUserMetadata().containsKey("key4"));
        Assert.assertEquals(metadata.getObjectContentLength(), 10);

        Assert.assertEquals(metadata, metadata.clone());

        Assert.assertEquals(metadata, new MetadataBuilder(metadata).build());
    }

    @Test
    public void case_insensitive_metadata() throws Exception {
        final String expectedContentType = "application/json";
        final Metadata metadata = new MetadataBuilder()
            .addMetadata("content-type", expectedContentType)
            .build();
        Assert.assertEquals(expectedContentType, metadata.getMetadata().get("Content-Type"));
        Assert.assertEquals(expectedContentType, metadata.getMetadata().get("content-type"));
    }
}
