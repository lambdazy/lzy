package ru.yandex.cloud.ml.platform.model.util.property;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class PropertyManagerStub<T> implements PropertyManager<T> {
    private T config;
    private final List<Consumer<T>> listeners = Collections.synchronizedList(new ArrayList<>());

    public PropertyManagerStub(T initialValue) {
        config = initialValue;
    }


    @Override
    public void addConfigChangedListener(Consumer<T> listener) {
        listeners.add(listener);
    }

    @Override
    public T getConfig() {
        return config;
    }

    @Override
    public void updateConfig(T config) {
        this.config = config;
        listeners.forEach(l -> l.accept(config));
    }
}
