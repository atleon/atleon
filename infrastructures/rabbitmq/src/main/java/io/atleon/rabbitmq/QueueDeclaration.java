package io.atleon.rabbitmq;

import java.util.Collections;
import java.util.Map;

public final class QueueDeclaration {

    private final String name;

    private final boolean durable;

    private final boolean exclusive;

    private final boolean autoDelete;

    private final Map<String, Object> arguments;

    private QueueDeclaration(String name) {
        this(name, true, false, false, Collections.emptyMap());
    }

    private QueueDeclaration(
            String name, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) {
        this.name = name;
        this.durable = durable;
        this.exclusive = exclusive;
        this.autoDelete = autoDelete;
        this.arguments = arguments;
    }

    public static QueueDeclaration named(String name) {
        return new QueueDeclaration(name);
    }

    public QueueDeclaration durable(boolean durable) {
        return new QueueDeclaration(name, durable, exclusive, autoDelete, arguments);
    }

    public QueueDeclaration exclusive(boolean exclusive) {
        return new QueueDeclaration(name, durable, exclusive, autoDelete, arguments);
    }

    public QueueDeclaration autoDelete(boolean autoDelete) {
        return new QueueDeclaration(name, durable, exclusive, autoDelete, arguments);
    }

    public QueueDeclaration arguments(Map<String, Object> arguments) {
        return new QueueDeclaration(name, durable, exclusive, autoDelete, arguments);
    }

    public String getName() {
        return name;
    }

    public boolean isDurable() {
        return durable;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }
}
