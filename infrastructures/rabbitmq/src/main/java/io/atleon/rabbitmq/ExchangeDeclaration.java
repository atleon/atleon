package io.atleon.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import java.util.Collections;
import java.util.Map;

public final class ExchangeDeclaration {

    private final String name;

    private final BuiltinExchangeType type;

    private final boolean durable;

    private final boolean autoDelete;

    private final Map<String, Object> arguments;

    private ExchangeDeclaration(String name, BuiltinExchangeType type) {
        this(name, type, true, false, Collections.emptyMap());
    }

    private ExchangeDeclaration(
            String name, BuiltinExchangeType type, boolean durable, boolean autoDelete, Map<String, Object> arguments) {
        this.name = name;
        this.type = type;
        this.durable = durable;
        this.autoDelete = autoDelete;
        this.arguments = arguments;
    }

    public static ExchangeDeclaration direct(String name) {
        return new ExchangeDeclaration(name, BuiltinExchangeType.DIRECT);
    }

    public static ExchangeDeclaration topic(String name) {
        return new ExchangeDeclaration(name, BuiltinExchangeType.TOPIC);
    }

    public static ExchangeDeclaration fanout(String name) {
        return new ExchangeDeclaration(name, BuiltinExchangeType.FANOUT);
    }

    public static ExchangeDeclaration headers(String name) {
        return new ExchangeDeclaration(name, BuiltinExchangeType.HEADERS);
    }

    public ExchangeDeclaration durable(boolean durable) {
        return new ExchangeDeclaration(name, type, durable, autoDelete, arguments);
    }

    public ExchangeDeclaration autoDelete(boolean autoDelete) {
        return new ExchangeDeclaration(name, type, durable, autoDelete, arguments);
    }

    public ExchangeDeclaration arguments(Map<String, Object> arguments) {
        return new ExchangeDeclaration(name, type, durable, autoDelete, arguments);
    }

    public String getName() {
        return name;
    }

    public BuiltinExchangeType getType() {
        return type;
    }

    public boolean isDurable() {
        return durable;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }
}
