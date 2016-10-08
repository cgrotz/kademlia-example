package de.cgrotz.kademlia.mapDb;

import de.cgrotz.kademlia.node.Key;
import de.cgrotz.kademlia.storage.LocalStorage;
import de.cgrotz.kademlia.storage.Value;
import org.mapdb.DB;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Created by Christoph on 06.10.2016.
 */
public class MapDbLocalStorage implements LocalStorage {
    private final ConcurrentMap<Key, Value> storage;

    public MapDbLocalStorage(ConcurrentMap<Key, Value> storage) {
        this.storage = storage;
    }

    @Override
    public void put(Key key, Value value) {
        storage.put(key, value);
    }

    @Override
    public Value get(Key key) {
        return storage.get(key);
    }

    @Override
    public boolean contains(Key key) {
        return storage.containsKey(key);
    }

    @Override
    public List<Key> getKeysBeforeTimestamp(long timestamp) {
        return storage.entrySet().parallelStream().filter( entry ->
                entry.getValue().getLastPublished() <= timestamp
        ).map(Map.Entry::getKey).collect(Collectors.toList());
    }
}
