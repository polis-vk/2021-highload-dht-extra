package ru.mail.polis;

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ModerateClusterDataTest extends TestBase {
    private static final int NODES = 300_000;
    private static final int CELLS = 10;
    public static final String LONG_KEY = "very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very-very" +
            "-long-key:%07d";

    private static final String[] keys = new String[CELLS];
    private static final String[] values = new String[CELLS];

    static {
        for (int i = 0; i < CELLS; i++) {
            keys[i] = String.format(LONG_KEY, i);
            values[i] = "value";
        }
    }

    @Test
    void consistent() {
        Map.Entry<Node, Iterator<Record>>[] records = new Map.Entry[NODES];
        for (int n = 0; n < NODES; n++) {
            //noinspection Convert2Lambda
            final Node node = new Node() {
                @Override
                public void update(final Record record) {
                    fail();
                }
            };
            final Iterator<Record> data = new Counter(node, 0, 1, CELLS);
            records[n] = Map.entry(node, data);
        }

        Iterator<Record> result = RepairingMerger.mergeAndRepair(Map.ofEntries(records));
        for (int i = 0; i < CELLS; i++) {
            final Record record = result.next();
            assertEquals(keys[i], record.key);
            assertEquals(values[i], record.value);
            assertEquals(i, record.ts);
        }
    }

    private static final class Counter implements Iterator<Record> {
        private final Node node;
        private int i;
        private final int step;
        private final int limit;

        private Counter(
                final Node node,
                final int start,
                final int step,
                final int limit) {
            this.i = start;
            this.node = node;
            this.step = step;
            this.limit = limit;
        }

        @Override
        public boolean hasNext() {
            return i < limit;
        }

        @Override
        public Record next() {
            assertTrue(hasNext());
            final Record result = new Record(node, keys[i], values[i], i);
            i += step;
            return result;
        }
    }
}
