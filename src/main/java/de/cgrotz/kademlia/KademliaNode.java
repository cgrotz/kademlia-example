package de.cgrotz.kademlia;

import de.cgrotz.kademlia.console.Console;
import de.cgrotz.kademlia.mapDb.MapDbLocalStorage;
import de.cgrotz.kademlia.node.Key;
import de.cgrotz.kademlia.storage.LocalStorage;
import de.cgrotz.kademlia.storage.Value;
import org.jetbrains.annotations.NotNull;
import org.mapdb.*;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Christoph on 06.10.2016.
 */
public class KademliaNode {

    public static void main(String[] args) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        DB db = DBMaker.fileDB(args[2]).make();

        LocalStorage localStorage = new MapDbLocalStorage((ConcurrentMap<Key, Value>)db
                .hashMap("storage")
                .keySerializer(new Serializer<Key>() {
                    @Override
                    public void serialize(@NotNull DataOutput2 out, @NotNull Key value) throws IOException {
                        out.writeUTF(value.toString());
                    }

                    @Override
                    public Key deserialize(@NotNull DataInput2 input, int available) throws IOException {
                        String line = input.readUTF();
                        return Key.build(line);
                    }
                })
                .valueSerializer(new Serializer<Value>() {
                    @Override
                    public void serialize(@NotNull DataOutput2 out, @NotNull Value value) throws IOException {
                        out.writeLong(value.getLastPublished());
                        out.writeUTF(value.getContent());
                    }

                    @Override
                    public Value deserialize(@NotNull DataInput2 input, int available) throws IOException {
                        long lastPublished = input.readLong();
                        String content = input.readUTF();
                        return Value.builder().lastPublished(lastPublished)
                                .content(content).build();
                    }
                })
                .createOrOpen());

        Kademlia kademlia = new Kademlia(Key.build(args[0]), args[1], localStorage);

        Console console = new Console(kademlia, db);
        kademlia.addEventListener("console", console);
        console.start();

        // After one minute and then every 5 minutes, run the key republish method
        scheduler.scheduleAtFixedRate(() -> {
            kademlia.republishKeys();
        },1,5, TimeUnit.MINUTES );


        // After five minutes and then every 60 minutes, refresh the buckets in the routing table
        scheduler.scheduleAtFixedRate(() -> {
            kademlia.refreshBuckets();
        },5,60, TimeUnit.MINUTES );
    }
}
