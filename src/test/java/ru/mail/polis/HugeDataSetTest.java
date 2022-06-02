package ru.mail.polis;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class HugeDataSetTest extends TestBase {
    private static final int NODES = 17;
    private static final int CELLS = 1_000_000;

    @Test
    void consistent() {
        final Map<Node, Iterator<Record>> iterators = new HashMap<>(NODES);
        for (int n = 0; n < NODES; n++) {
            //noinspection Convert2Lambda
            final Node node = new Node() {
                @Override
                public void update(final Record record) {
                    fail();
                }
            };
            final Iterator<Record> data = new Counter(node, 0, 1, CELLS);
            assertNull(iterators.put(node, data));
        }

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators);
        for (int i = 0; i < CELLS; i++) {
            final Record record = result.next();
            assertEquals(key(i), record.key);
            assertEquals(value(i), record.value);
            assertEquals(i, record.ts);
        }
    }

    @Test
    void striped() {
        final Map<Node, Iterator<Record>> iterators = new HashMap<>();
        for (int n = 0; n < NODES; n++) {
            final int start = n;

            final Node node = new Node() {
                private int expected = 0;

                @Override
                public void update(final Record record) {
                    // Advance the cell we own
                    if ((expected - start) % NODES == 0) {
                        expected++;
                    }

                    // Check the cell
                    assertNotEquals(this, record.node);
                    assertEquals(key(expected), record.key);
                    assertEquals(value(expected), record.value);
                    assertEquals(expected, record.ts);

                    // Expect next repair
                    expected++;
                }
            };

            final Iterator<Record> data = new Counter(node, start, NODES, CELLS);
            assertNull(iterators.put(node, data));
        }

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators);
        for (int i = 0; i < CELLS; i++) {
            final Record record = result.next();
            assertEquals(key(i), record.key);
            assertEquals(value(i), record.value);
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
            final Record result = new Record(node, key(i), value(i), i);
            i += step;
            return result;
        }
    }
}
