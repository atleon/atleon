package io.atleon.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParsingTest {

    @Test
    public void toInteger_givenValidInput_expectsParsedOutput() {
        assertEquals(1, Parsing.toInteger("1"));
        assertEquals(101, Parsing.toInteger("101"));
    }

    @Test
    public void toInteger_givenInvalidInput_expectsException() {
        assertThrows(IllegalArgumentException.class, () -> Parsing.toInteger(""));
        assertThrows(IllegalArgumentException.class, () -> Parsing.toInteger("invalid"));
    }

    @Test
    public void toLong_givenValidInput_expectsParsedOutput() {
        assertEquals(1L, Parsing.toLong("1"));
        assertEquals(101L, Parsing.toLong("101"));
    }

    @Test
    public void toLong_givenInvalidInput_expectsException() {
        assertThrows(IllegalArgumentException.class, () -> Parsing.toLong(""));
        assertThrows(IllegalArgumentException.class, () -> Parsing.toLong("invalid"));
    }
}
