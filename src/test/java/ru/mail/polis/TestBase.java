package ru.mail.polis;

import org.opentest4j.AssertionFailedError;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBase {

    static String key(final int i) {
        assertTrue(0 <= i && i < 10_000_000);
        return String.format("key:%07d", i);
    }

    static String value(final int i) {
        assertTrue(0 <= i && i < 10_000_000);
        return String.format("value:%07d", i);
    }

    static Map<Node, Iterator<Record>> iterators(SimpleNode... nodes) {
        Map<Node, Iterator<Record>> records = new HashMap<>();
        for (SimpleNode node : nodes) {
            records.put(node, node.storage.iterator());
        }
        return records;
    }

    static void assertUpdated(SimpleNode node, String... keyValues) {
        assertKeyValues(node.updated.iterator(), keyValues);
    }

    static void assertKeyValues(Iterator<Record> iterator, String... keyValues) {
        List<Record> records = new ArrayList<>();
        iterator.forEachRemaining(records::add);
        iterator = records.iterator();

        try {
            assertEquals(keyValues.length / 2, records.size());
            int p = 0;
            while (iterator.hasNext()) {
                Record next = iterator.next();
                assertEquals(keyValues[p * 2], next.key);
                assertEquals(keyValues[p * 2 + 1], next.value);
                p++;
            }
        } catch (AssertionFailedError e) {
            throw new AssertionFailedError("Records: " + records, e);
        }
    }

    static SimpleNode node(String name) {
        return new SimpleNode(name);
    }

    final static class SimpleNode implements Node {
        private final String name;
        private final List<Record> updated = new ArrayList<>();
        private final List<Record> storage = new ArrayList<>();

        private SimpleNode(String name) {
            this.name = name;
        }

        @Override
        public void update(Record record) {
            updated.add(record);
        }

        SimpleNode with(String key, String value, long ts) {
            storage.add(new Record(this, key, value, ts));
            return this;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
