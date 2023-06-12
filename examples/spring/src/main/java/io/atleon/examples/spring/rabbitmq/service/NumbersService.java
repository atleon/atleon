package io.atleon.examples.spring.rabbitmq.service;

import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
public class NumbersService {

    private final Consumer<Number> specialNumberConsumer;

    public NumbersService(Consumer<Number> specialNumberConsumer) {
        this.specialNumberConsumer = specialNumberConsumer;
    }

    public boolean isPrime(Number number) {
        long safeValue = Math.abs(number.longValue());
        for (long i = 2; i <= Math.sqrt(safeValue); i++) {
            if (safeValue % i == 0) {
                return false;
            }
        }
        specialNumberConsumer.accept(number);
        return safeValue > 0;
    }
}
