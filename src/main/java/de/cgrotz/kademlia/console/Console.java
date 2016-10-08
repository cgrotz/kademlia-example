package de.cgrotz.kademlia.console;

import de.cgrotz.kademlia.Kademlia;
import de.cgrotz.kademlia.config.UdpListener;
import de.cgrotz.kademlia.events.Event;
import de.cgrotz.kademlia.node.Key;
import de.cgrotz.kademlia.node.Node;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by Christoph on 06.10.2016.
 */
public class Console implements Consumer<Event> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Console.class);

    private static final String BANNER = "  _  __          _____  ______ __  __ _      _____          \n" +
            " | |/ /    /\\   |  __ \\|  ____|  \\/  | |    |_   _|   /\\    \n" +
            " | ' /    /  \\  | |  | | |__  | \\  / | |      | |    /  \\   \n" +
            " |  <    / /\\ \\ | |  | |  __| | |\\/| | |      | |   / /\\ \\  \n" +
            " | . \\  / ____ \\| |__| | |____| |  | | |____ _| |_ / ____ \\ \n" +
            " |_|\\_\\/_/    \\_\\_____/|______|_|  |_|______|_____/_/    \\_\\\n" +
            "                                                            \n" +
            " 1.0.1";
    private static final String LINE_START = "> ";
    private static final String COMMAND_RESPONSE = ":= ";

    private final Kademlia kademlia;

    private final Map<String, Function<String[], String>> commands = new HashMap<>();
    private final MessageDigest digest;

    public Console(Kademlia kademlia, DB db) {
        this.kademlia = kademlia;
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        this.commands.put("HELP", (args) -> "Available Commands: \r\n HELP\t This page");
        this.commands.put("BOOTSTRAP", (args) -> {
            kademlia.bootstrap(Node.builder().advertisedListener(
                    new UdpListener(args[1])
            ).build());
            return "Connected to "+args[1];
        });
        this.commands.put("STATUS", (args) -> {
            String output = "% This node Id:"+kademlia.getLocalNode().getId()+"\r\n";
            List<Node> allNodes = kademlia.getRoutingTable().getBucketStream().flatMap(bucket -> bucket.getNodes().stream()).collect(Collectors.toList());
            for(Node node: allNodes) {
                output += "=>"+node;
            }
            return output;
        });
        this.commands.put("PUT", (args) -> {
            kademlia.put(Key.build(args[1]), args[2]);
            return "Stored value";
        });
        this.commands.put("PUT", (args) -> {
            if(args.length > 2) {
                Key key = Key.build(args[1]);
                kademlia.put(key, args[2]);
                return "Stored value at "+key;
            }
            else {
                byte[] hash = digest.digest(args[1].getBytes(StandardCharsets.UTF_8));
                Key key = new Key(Arrays.copyOfRange(hash, 0, 160/8));
                kademlia.put(key, args[1]);
                return "Stored value at "+key;
            }
        });
        this.commands.put("GET", (args) -> {
            return kademlia.get(Key.build(args[1]));
        });
        this.commands.put("EXIT", (args) -> {
           db.close();
           kademlia.close();
           Runtime.getRuntime().exit(1);
           return "closing";
        });
    }

    public void start() {
        System.out.println(BANNER);
        System.out.print(LINE_START);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while (true) {
                line = br.readLine();
                parseLine(line, System.out);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void parseLine(String line, PrintStream out) {
        String[] lines = line.split(" ");
        Function<String[], String> handler = commands.get(lines[0].toUpperCase());
        if(handler == null) {
            System.out.println(COMMAND_RESPONSE + "Command '"+lines[0]+ "' not found! Enter HELP for information.");
        }
        else {
            String reply = handler.apply(lines);
            System.out.println(COMMAND_RESPONSE + reply);
        }
        // Print LINE_START for next command
        System.out.print(LINE_START);
    }

    @Override
    public void accept(Event event) {
        System.out.println("% "+event);
    }
}
