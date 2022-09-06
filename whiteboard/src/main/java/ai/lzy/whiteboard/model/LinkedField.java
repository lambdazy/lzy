package ai.lzy.whiteboard.model;

import ai.lzy.model.data.DataSchema;
import java.util.Objects;

public class LinkedField extends Field {
    private final String storageUri;
    private final DataSchema schema;

    public LinkedField(String name, Status status, String storageUri, DataSchema schema) {
        super(name, status);
        this.storageUri = storageUri;
        this.schema = schema;
    }

    public String storageUri() {
        return storageUri;
    }

    public DataSchema schema() {
        return schema;
    }

    @Override
    public String toString() {
        return "LinkedField{" +
               "name='" + name + '\'' +
               ", status=" + status.name() +
               ", storageUri='" + storageUri + '\'' +
               ", schema=" + schema +
               '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LinkedField that = (LinkedField) o;
        return storageUri.equals(that.storageUri) && schema.equals(that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), storageUri, schema);
    }
}
