package io.atleon.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParsingTest {

    @Test
    public void toInteger_givenValidInput_expectsParsedOutput() {
        assertEquals(Integer.valueOf(1), Parsing.toInteger("1"));
        assertEquals(Integer.valueOf(101), Parsing.toInteger("101"));
    }

    @Test
    public void toInteger_givenInvalidInput_expectsException() {
        assertThrows(IllegalArgumentException.class, () -> Parsing.toInteger(""));
        assertThrows(IllegalArgumentException.class, () -> Parsing.toInteger("invalid"));
    }

    @Test
    public void toLong_givenValidInput_expectsParsedOutput() {
        assertEquals(Long.valueOf(1), Parsing.toLong("1"));
        assertEquals(Long.valueOf(101), Parsing.toLong("101"));
    }

    @Test
    public void toLong_givenInvalidInput_expectsException() {
        assertThrows(IllegalArgumentException.class, () -> Parsing.toLong(""));
        assertThrows(IllegalArgumentException.class, () -> Parsing.toLong("invalid"));
    }
}
