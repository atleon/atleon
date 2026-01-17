package io.atleon.kafka;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MachineDataTest {

    @Test
    public void fromByteBuffer_givenSerializedMachineData_expectsEqualDeserializedMachineData() {
        MachineData machineData = MachineData.birth();

        ByteBuffer byteBuffer = machineData.toByteBuffer();

        MachineData result = MachineData.fromByteBuffer(byteBuffer);

        assertEquals(machineData, result);
    }
}
