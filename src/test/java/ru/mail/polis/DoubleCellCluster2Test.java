package ru.mail.polis;

import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class DoubleCellCluster2Test extends TestBase {
    @Test
    void values1() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100)
                .with("b", "2", 101);
        SimpleNode n2 = node("n2");

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result,
                "a", "1",
                "b", "2"
        );

        assertUpdated(n1);
        assertUpdated(n2,
                "a", "1",
                "b", "2");
    }

    @Test
    void values2() {
        SimpleNode n1 = node("n1");
        SimpleNode n2 = node("n2")
                .with("a", "1", 100)
                .with("b", "2", 101);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result,
                "a", "1",
                "b", "2"
        );

        assertUpdated(n1,
                "a", "1",
                "b", "2");
        assertUpdated(n2);
    }

    @Test
    void fresherValue1() {
        SimpleNode n1 = node("n1")
                .with("a", "a1", 101)
                .with("b", "b1", 101);
        SimpleNode n2 = node("n2")
                .with("a", "a2", 100)
                .with("b", "b2", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result,
                "a", "a1",
                "b", "b1"
        );

        assertUpdated(n1);
        assertUpdated(n2,
                "a", "a1",
                "b", "b1");
    }

    @Test
    void fresherValue2() {
        SimpleNode n1 = node("n1")
                .with("a", "a1", 100)
                .with("b", "b1", 100);
        SimpleNode n2 = node("n2")
                .with("a", "a2", 101)
                .with("b", "b2", 101);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result,
                "a", "a2",
                "b", "b2"
        );

        assertUpdated(n1,
                "a", "a2",
                "b", "b2");
        assertUpdated(n2);
    }

    @Test
    void lesserValue1() {
        SimpleNode n1 = node("n1")
                .with("a", "a1", 100)
                .with("b", "b1", 100);
        SimpleNode n2 = node("n2")
                .with("a", "a2", 100)
                .with("b", "b2", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result,
                "a", "a1",
                "b", "b1"
        );

        assertUpdated(n1);
        assertUpdated(n2,
                "a", "a1",
                "b", "b1");
    }

    @Test
    void lesserValue2() {
        SimpleNode n1 = node("n1")
                .with("a", "a11", 100)
                .with("b", "b11", 100);
        SimpleNode n2 = node("n2")
                .with("a", "a1", 100)
                .with("b", "b1", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result,
                "a", "a1",
                "b", "b1"
        );

        assertUpdated(n1,
                "a", "a1",
                "b", "b1");
        assertUpdated(n2);
    }

    @Test
    void tombstones1() {
        SimpleNode n1 = node("n1")
                .with("a", null, 100)
                .with("b", null, 101);
        SimpleNode n2 = node("n2");

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result);

        assertUpdated(n1);
        assertUpdated(n2,
                "a", null,
                "b", null);
    }

    @Test
    void tombstone2() {
        SimpleNode n1 = node("n1");
        SimpleNode n2 = node("n2")
                .with("a", null, 100)
                .with("b", null, 101);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result);

        assertUpdated(n1,
                "a", null,
                "b", null);
        assertUpdated(n2);
    }

    @Test
    void fresherTombstone1() {
        SimpleNode n1 = node("n1")
                .with("a", null, 100)
                .with("b", null, 101);
        SimpleNode n2 = node("n2")
                .with("a", "2", 100)
                .with("b", "2", 101);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result);

        assertUpdated(n1);
        assertUpdated(n2,
                "a", null,
                "b", null);
    }

    @Test
    void fresherTombstone2() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100)
                .with("b", "2", 101);
        SimpleNode n2 = node("n2")
                .with("a", null, 100)
                .with("b", null, 101);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result);

        assertUpdated(n1,
                "a", null,
                "b", null);
        assertUpdated(n2);
    }
}
