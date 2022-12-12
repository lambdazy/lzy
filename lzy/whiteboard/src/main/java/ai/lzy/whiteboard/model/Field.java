package ai.lzy.whiteboard.model;

import java.util.Objects;

public class Field {
    protected final String name;
    protected final Status status;

    public Field(String name, Status status) {
        this.name = name;
        this.status = status;
    }

    public String name() {
        return name;
    }

    public Status status() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Field field = (Field) o;
        return name.equals(field.name) && status == field.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, status);
    }

    @Override
    public String toString() {
        return "Field{" +
               "name='" + name + '\'' +
               ", status=" + status.name() +
               '}';
    }

    public enum Status {
        CREATED,
        FINALIZED,
    }

}
