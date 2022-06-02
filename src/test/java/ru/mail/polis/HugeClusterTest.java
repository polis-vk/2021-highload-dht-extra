package ru.mail.polis;

import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HugeClusterTest extends TestBase {
    private static final int NODES = 16384;
    private static final int KEYS = 64;
    private static final String[] keys = new String[KEYS];
    private static final String[] values = new String[KEYS];
    static {
        for (int i = 0; i < KEYS; i++) {
            keys[i] = key(i);
            values[i] = value(i);
        }
    }

    @Test
    void consistent() {
        final SimpleNode[] nodes = new SimpleNode[NODES];
        for (int i = 0; i < nodes.length; i++) {
            final SimpleNode node = node("n" + i);
            for (int j = 0; j < KEYS; j++) {
                node.with(keys[j], values[j], j);
            }
            nodes[i] = node;
        }

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(nodes));
        for (int i = 0; i < KEYS; i++) {
            final Record record = result.next();
            assertEquals(keys[i], record.key);
            assertEquals(values[i], record.value);
            assertEquals(i, record.ts);
        }

        // Nothing updated
        for (final SimpleNode node : nodes) {
            assertUpdated(node);
        }
    }

    @Test
    void allEmptyExceptOne() {
        // All empty
        final SimpleNode[] nodes = new SimpleNode[NODES];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = node("n" + i);
        }

        // Except the 0th one
        for (int i = 0; i < KEYS; i++) {
            nodes[0].with(keys[i], values[i], i);
        }

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(nodes));
        for (int i = 0; i < KEYS; i++) {
            final Record record = result.next();
            assertEquals(keys[i], record.key);
            assertEquals(values[i], record.value);
            assertEquals(i, record.ts);
        }

        // Everything repaired
        final String[] dataset = new String[2 * KEYS];
        for (int i = 0; i < KEYS; i++) {
            dataset[i * 2] = keys[i];
            dataset[i * 2 + 1] = values[i];
        }
        for (int i = 1; i < NODES; i++) {
            assertUpdated(nodes[i], dataset);
        }

        // Except the 0th one
        assertUpdated(nodes[0]);
    }
}
