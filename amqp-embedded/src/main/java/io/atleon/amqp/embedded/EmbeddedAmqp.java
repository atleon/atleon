package io.atleon.amqp.embedded;

import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <a href="https://cwiki.apache.org/confluence/display/qpid/How+to+embed+Qpid+Broker-J">Reference</a>
 */
public final class EmbeddedAmqp {

    public static final int DEFAULT_PORT = 5672;

    private static final String PORT_PROPERTY = "port";

    private static final String USERNAME_PROPERTY = "username";

    private static final String PASSWORD_PROPERTY = "password";

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedAmqp.class);

    private static final Pattern SEMVER_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)\\..*");

    private static EmbeddedAmqpConfig config;

    public static EmbeddedAmqpConfig start() {
        return start(DEFAULT_PORT);
    }

    public static synchronized EmbeddedAmqpConfig start(int port) {
        return config == null ? config = initializeAmqp(port) : config;
    }

    private static EmbeddedAmqpConfig initializeAmqp(int port) {
        EmbeddedAmqpConfig config = new EmbeddedAmqpConfig("localhost", port, "/", "guest", "guest");
        startLocalBroker(config);
        return config;
    }

    private static void startLocalBroker(EmbeddedAmqpConfig config) {
        try {
            LOGGER.info("BEGINNING STARTUP OF LOCAL AMQP BROKER");
            Path tempDirectory = Files.createTempDirectory(EmbeddedAmqp.class.getSimpleName() + "_" + System.currentTimeMillis());
            System.getProperties().putIfAbsent("derby.stream.error.file", new File(tempDirectory.toFile(), "derby.log").getAbsolutePath());
            Map<String, Object> attributes = createAttributes(config, tempDirectory);
            SystemLauncher systemLauncher = new SystemLauncher();
            systemLauncher.startup(attributes);
            LOGGER.info("FINISHED STARTUP OF LOCAL AMQP BROKER");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start local Broker: " + e);
        }
    }

    private static Map<String, Object> createAttributes(EmbeddedAmqpConfig config, Path tempDirectory) throws Exception {
        Map<String, String> context = new HashMap<>();
        context.put(PORT_PROPERTY, Integer.toString(config.getPort()));
        context.put(USERNAME_PROPERTY, config.getUsername());
        context.put(PASSWORD_PROPERTY, config.getPassword());
        context.put(SystemConfig.QPID_WORK_DIR, tempDirectory.toString());

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(SystemConfig.TYPE, "JSON");
        attributes.put(SystemConfig.INITIAL_CONFIGURATION_LOCATION, createAmqpConfig(tempDirectory).getCanonicalPath());
        attributes.put(SystemConfig.CONTEXT, context);
        return attributes;
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
        configWriter.println("    \"ports\": [{");
        configWriter.println("        \"name\": \"AMQP\",");
        configWriter.println("        \"port\": \"${" + PORT_PROPERTY + "}\",");
        configWriter.println("        \"transports\": [\"TCP\"],");
        configWriter.println("        \"authenticationProvider\": \"plain\",");
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
