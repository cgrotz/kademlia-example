package de.cgrotz.kademlia;

import de.cgrotz.kademlia.console.Console;
import de.cgrotz.kademlia.node.Key;

/**
 * Created by Christoph on 06.10.2016.
 */
public class KademliaNode {

    public static void main(String[] args) {
        Kademlia kademlia = new Kademlia(Key.build(args[0]), args[1]);
        Console console = new Console(kademlia);
        kademlia.addEventListener("console", console);
        console.start();
    }
}
