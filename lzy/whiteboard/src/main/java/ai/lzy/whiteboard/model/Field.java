package ai.lzy.whiteboard.model;

import ai.lzy.model.DataScheme;


public record Field(String name, DataScheme schema) {

    @Override
    public String toString() {
        return "Field{" +
            "name='" + name + '\'' +
            ", schema=" + schema() +
            '}';
    }
}
