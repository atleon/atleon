package io.atleon.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public final class TestKafkaSetup {

    private TestKafkaSetup() {}

    public static void createTopics(String bootstrapServers, NewTopic... newTopics) {
        createTopics(bootstrapServers, Arrays.asList(newTopics));
    }

    public static void createTopics(String bootstrapServers, Collection<NewTopic> newTopics) {
        Map<String, Object> adminConfig =
                Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (Admin admin = Admin.create(adminConfig)) {
            admin.createTopics(newTopics).all().get();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create topics", e);
        }
    }
}
