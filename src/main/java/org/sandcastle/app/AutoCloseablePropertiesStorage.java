package main.java.org.sandcastle.app;

import static javascalautils.TryCompanion.Try;

import javascalautils.Option;
import javascalautils.Try;
import javascalautils.Unit;
import org.dmonix.zookeeper.PropertiesStorage;
import org.dmonix.zookeeper.PropertySet;

import java.util.List;
import java.util.function.Function;

final class AutoCloseablePropertiesStorage implements PropertiesStorage {

    private PropertiesStorage propertiesStorage;

    AutoCloseablePropertiesStorage(PropertiesStorage propertiesStorage) {
        this.propertiesStorage = propertiesStorage;
    }

    @Override
    public Try<Option<PropertySet>> get(String name) {
        return invoke(storage -> storage.get(name));
    }

    @Override
    public Try<Unit> store(PropertySet propertySet) {
        return invoke(storage -> storage.store(propertySet));
    }

    @Override
    public Try<Unit> delete(String name) {
        return invoke(storage -> storage.delete(name));
    }

    @Override
    public Try<List<String>> propertySets() {
        return invoke(PropertiesStorage::propertySets);
    }

    @Override
    public void close() {
        Try(() -> propertiesStorage.close());
    }

    private <R> R invoke(Function<PropertiesStorage, R> func) {
        try {
            return func.apply(propertiesStorage);
        } finally {
            close();
        }
    }
}
