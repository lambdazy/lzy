package ru.yandex.qe.s3.transfer.meta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Established by terry on 20.01.16.
 */
@NotThreadSafe
public class MetadataBuilder {

    private Map<String, Object> metadata = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private Map<String, Object> userMetadata = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private List<Object> aclObjects = new ArrayList<>(2);

    private long objectContentLength = Metadata.UNDEFINED_LENGTH;

    public MetadataBuilder() {
    }

    public MetadataBuilder(Metadata baseMetadata) {
        addMetadata(baseMetadata.getMetadata());
        addUserMetadata(baseMetadata.getUserMetadata());
        addAclObjects(baseMetadata.getAclObjects());
        setObjectContentLength(baseMetadata.getObjectContentLength());
    }

    public MetadataBuilder setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    public MetadataBuilder addMetadata(Map<String, Object> metadata) {
        this.metadata.putAll(metadata);
        return this;
    }

    public MetadataBuilder addMetadata(String key, Object value) {
        this.metadata.put(key, value);
        return this;
    }

    public MetadataBuilder setUserMetadata(Map<String, Object> userMetadata) {
        this.userMetadata = userMetadata;
        return this;
    }

    public MetadataBuilder addUserMetadata(Map<String, Object> userMetadata) {
        this.userMetadata.putAll(userMetadata);
        return this;
    }

    public MetadataBuilder addUserMetadata(String key, Object value) {
        this.userMetadata.put(key, value);
        return this;
    }

    public MetadataBuilder setAclObjects(List<Object> aclObjects) {
        this.aclObjects = aclObjects;
        return this;
    }

    public MetadataBuilder addAclObject(Object aclObject) {
        this.aclObjects.add(aclObject);
        return this;
    }

    public MetadataBuilder addAclObjects(List<Object> aclObjects) {
        this.aclObjects.addAll(aclObjects);
        return this;
    }

    public MetadataBuilder setObjectContentLength(long objectContentLength) {
        this.objectContentLength = objectContentLength;
        return this;
    }

    public Metadata build() {
        return new Metadata(metadata, userMetadata, aclObjects, objectContentLength);
    }
}
