package io.github.phuongtran.androidoutbox

import org.junit.Assert.assertEquals
import org.junit.Test

class OutboxLogFileFormatTest {
    @Test
    fun `uses stable tab-delimited field contract`() {
        assertEquals('\t', OutboxLogFileFormat.FIELD_DELIMITER)
        assertEquals(5, OutboxLogFileFormat.FIELD_COUNT)
        assertEquals(0, OutboxLogFileFormat.FIELD_INDEX_WALL_TIME_MS)
        assertEquals(1, OutboxLogFileFormat.FIELD_INDEX_SEQUENCE)
        assertEquals(2, OutboxLogFileFormat.FIELD_INDEX_LEVEL)
        assertEquals(3, OutboxLogFileFormat.FIELD_INDEX_CATEGORY)
        assertEquals(4, OutboxLogFileFormat.FIELD_INDEX_PAYLOAD)
    }
}
