package io.atleon.amqp.embedded;

import org.apache.qpid.server.Main;
import org.apache.qpid.server.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class EmbeddedAmqp {

    public static final int DEFAULT_PORT = 5672;

    public static final String HOST_PROPERTY = "host";

    public static final String PORT_PROPERTY = "port";

    public static final String VIRTUAL_HOST_PROPERTY = "virtual-host";

    public static final String USERNAME_PROPERTY = "username";

    public static final String PASSWORD_PROPERTY = "password";

    public static final String SSL_PROPERTY = "ssl";

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedAmqp.class);

    private static final Pattern SEMVER_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)\\..*");

    private static Map<String, String> brokerOptions;

    public static Map<String, ?> start() {
        return start(DEFAULT_PORT);
    }

    public static synchronized Map<String, ?> start(int port) {
        return brokerOptions == null ? brokerOptions = initializeAmqp(port) : brokerOptions;
    }

    private static Map<String, String> initializeAmqp(int port) {
        Map<String, String> brokerOptions = createBrokerOptions(port);
        startLocalBroker(brokerOptions);
        return brokerOptions;
    }

    private static Map<String, String> createBrokerOptions(int port) {
        Map<String, String> brokerOptions = new HashMap<>();
        brokerOptions.put(HOST_PROPERTY, "localhost");
        brokerOptions.put(PORT_PROPERTY, Integer.toString(port));
        brokerOptions.put(VIRTUAL_HOST_PROPERTY, "/");
        brokerOptions.put(USERNAME_PROPERTY, "guest");
        brokerOptions.put(PASSWORD_PROPERTY, "guest");
        brokerOptions.put(SSL_PROPERTY, "disabled");
        return brokerOptions;
    }

    private static void startLocalBroker(Map<String, String> brokerOptions) {
        try {
            LOGGER.info("BEGINNING STARTUP OF LOCAL AMQP BROKER");
            Path tempDirectory = Files.createTempDirectory(EmbeddedAmqp.class.getSimpleName() + "_" + System.currentTimeMillis());
            List<String> localBrokerArguments = createLocalBrokerArguments(brokerOptions, tempDirectory);
            System.getProperties().putIfAbsent("derby.stream.error.file", new File(tempDirectory.toFile(), "derby.log").getAbsolutePath());
            org.apache.qpid.server.Main.main(localBrokerArguments.toArray(new String[0]));
            LOGGER.info("FINISHED STARTUP OF LOCAL AMQP BROKER");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start local Broker: " + e);
        }
    }

    private static List<String> createLocalBrokerArguments(Map<String, String> brokerOptions, Path tempDirectory) throws Exception {
        List<String> brokerArguments = new ArrayList<>(Arrays.asList(
            "--initial-config-path", createAmqpConfig(tempDirectory).getCanonicalPath(),
            "--config-property", String.format("%s=%s", SystemConfig.QPID_WORK_DIR, tempDirectory.toString()),
            "--config-property", String.format("%s=%s", Main.PROPERTY_QPID_HOME, tempDirectory.toString())));
        brokerOptions.forEach((key, value) -> brokerArguments.addAll(Arrays.asList(
            "--config-property", String.format("%s=%s", key, value))));
        return brokerArguments;
    }

    private static File createAmqpConfig(Path directory) throws Exception {
        File configFile = Files.createTempFile(directory, "amqp", SystemConfig.DEFAULT_INITIAL_CONFIG_NAME).toFile();
        PrintWriter configWriter = new PrintWriter(configFile);
        configWriter.println("{");
        configWriter.println("    \"name\": \"broker\",");
        configWriter.println("    \"modelVersion\": \"" + deduceQpidMajorMinorVersion() + "\",");
        configWriter.println("    \"virtualhostnodes\": [{");
        configWriter.println("        \"type\": \"Memory\",");
        configWriter.println("        \"name\": \"default\",");
        configWriter.println("        \"defaultVirtualHostNode\": \"true\",");
        configWriter.println("        \"virtualHostInitialConfiguration\": \"{\\\"type\\\": \\\"Memory\\\"}\"");
        configWriter.println("    }],");
        configWriter.println("    \"authenticationproviders\": [{");
        configWriter.println("        \"type\": \"Plain\",");
        configWriter.println("        \"name\": \"plain\",");
        configWriter.println("        \"users\": [{");
        configWriter.println("            \"type\": \"managed\",");
        configWriter.println("            \"name\": \"${" + USERNAME_PROPERTY + "}\",");
        configWriter.println("            \"password\": \"${" + PASSWORD_PROPERTY + "}\"");
        configWriter.println("        }],");
        configWriter.println("        \"secureOnlyMechanisms\": []");
        configWriter.println("    }],");
        configWriter.println("    \"keystores\": [{");
        configWriter.println("        \"type\": \"AutoGeneratedSelfSigned\",");
        configWriter.println("        \"name\": \"default\"");
        configWriter.println("    }],");
        configWriter.println("    \"ports\": [{");
        configWriter.println("        \"name\": \"AMQP\",");
        configWriter.println("        \"port\": \"${" + PORT_PROPERTY + "}\",");
        configWriter.println("        \"transports\": [\"TCP\"],");
        configWriter.println("        \"authenticationProvider\": \"plain\",");
        configWriter.println("        \"keyStore\": \"default\",");
        configWriter.println("        \"virtualhostaliases\": [{");
        configWriter.println("            \"type\": \"defaultAlias\",");
        configWriter.println("            \"name\": \"defaultAlias\"");
        configWriter.println("        }, {");
        configWriter.println("            \"type\": \"hostnameAlias\",");
        configWriter.println("            \"name\": \"hostnameAlias\"");
        configWriter.println("        }, {");
        configWriter.println("            \"type\": \"nameAlias\",");
        configWriter.println("            \"name\": \"nameAlias\"");
        configWriter.println("        }]");
        configWriter.println("    }]");
        configWriter.println("}");
        configWriter.flush();
        configWriter.close();
        return configFile;
    }

    private static String deduceQpidMajorMinorVersion() {
        String version = SystemConfig.class.getPackage().getImplementationVersion();
        Matcher matcher = SEMVER_PATTERN.matcher(version);
        return matcher.find() ? String.format("%s.%s", matcher.group(1), matcher.group(2)) : "7.0";
    }
}
