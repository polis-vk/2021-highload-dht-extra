package ru.mail.polis;

import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class SingleCellCluster2Test extends TestBase {
    @Test
    void value1() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100);
        SimpleNode n2 = node("n2");

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result,
                "a", "1"
        );

        assertUpdated(n1);
        assertUpdated(n2, "a", "1");
    }

    @Test
    void value2() {
        SimpleNode n1 = node("n1");
        SimpleNode n2 = node("n2")
                .with("a", "1", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result,
                "a", "1"
        );

        assertUpdated(n1, "a", "1");
        assertUpdated(n2);
    }

    @Test
    void fresherValue1() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 101);
        SimpleNode n2 = node("n2")
                .with("a", "2", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result,
                "a", "1"
        );

        assertUpdated(n1);
        assertUpdated(n2, "a", "1");
    }

    @Test
    void fresherValue2() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100);
        SimpleNode n2 = node("n2")
                .with("a", "2", 101);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result,
                "a", "2"
        );

        assertUpdated(n1, "a", "2");
        assertUpdated(n2);
    }

    @Test
    void lesserValue1() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100);
        SimpleNode n2 = node("n2")
                .with("a", "2", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result,
                "a", "1"
        );

        assertUpdated(n1);
        assertUpdated(n2, "a", "1");
    }

    @Test
    void lesserValue2() {
        SimpleNode n1 = node("n1")
                .with("a", "11", 100);
        SimpleNode n2 = node("n2")
                .with("a", "1", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result,
                "a", "1"
        );

        assertUpdated(n1, "a", "1");
        assertUpdated(n2);
    }

    @Test
    void tombstone1() {
        SimpleNode n1 = node("n1")
                .with("a", null, 100);
        SimpleNode n2 = node("n2");

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result);

        assertUpdated(n1);
        assertUpdated(n2, "a", null);
    }

    @Test
    void tombstone2() {
        SimpleNode n1 = node("n1");
        SimpleNode n2 = node("n2")
                .with("a", null, 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result);

        assertUpdated(n1, "a", null);
        assertUpdated(n2);
    }

    @Test
    void fresherTombstone1() {
        SimpleNode n1 = node("n1")
                .with("a", null, 100);
        SimpleNode n2 = node("n2")
                .with("a", "2", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result);

        assertUpdated(n1);
        assertUpdated(n2, "a", null);
    }

    @Test
    void fresherTombstone2() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100);
        SimpleNode n2 = node("n2")
                .with("a", null, 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result);

        assertUpdated(n1, "a", null);
        assertUpdated(n2);
    }
}
