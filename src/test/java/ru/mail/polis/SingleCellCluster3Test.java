package ru.mail.polis;

import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class SingleCellCluster3Test extends TestBase {
    @Test
    void value1() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100);
        SimpleNode n2 = node("n2");
        SimpleNode n3 = node("n3");

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result,
                "a", "1"
        );

        assertUpdated(n1);
        assertUpdated(n2, "a", "1");
        assertUpdated(n3, "a", "1");
    }

    @Test
    void value2() {
        SimpleNode n1 = node("n1");
        SimpleNode n2 = node("n2")
                .with("a", "1", 100);
        SimpleNode n3 = node("n3");

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result,
                "a", "1"
        );

        assertUpdated(n1, "a", "1");
        assertUpdated(n2);
        assertUpdated(n3, "a", "1");
    }

    @Test
    void value3() {
        SimpleNode n1 = node("n1");
        SimpleNode n2 = node("n2");
        SimpleNode n3 = node("n3")
                .with("a", "1", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result,
                "a", "1"
        );

        assertUpdated(n1, "a", "1");
        assertUpdated(n2, "a", "1");
        assertUpdated(n3);
    }

    @Test
    void fresherValue1() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 101);
        SimpleNode n2 = node("n2")
                .with("a", "2", 100);
        SimpleNode n3 = node("n3")
                .with("a", "3", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result,
                "a", "1"
        );

        assertUpdated(n1);
        assertUpdated(n2, "a", "1");
        assertUpdated(n3, "a", "1");
    }

    @Test
    void fresherValue2() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100);
        SimpleNode n2 = node("n2")
                .with("a", "2", 101);
        SimpleNode n3 = node("n3")
                .with("a", "3", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result,
                "a", "2"
        );

        assertUpdated(n1, "a", "2");
        assertUpdated(n2);
        assertUpdated(n3, "a", "2");
    }

    @Test
    void fresherValue3() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100);
        SimpleNode n2 = node("n2")
                .with("a", "2", 100);
        SimpleNode n3 = node("n3")
                .with("a", "3", 101);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result,
                "a", "3"
        );

        assertUpdated(n1, "a", "3");
        assertUpdated(n2, "a", "3");
        assertUpdated(n3);
    }

    @Test
    void lesserValue1() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100);
        SimpleNode n2 = node("n2")
                .with("a", "2", 100);
        SimpleNode n3 = node("n3")
                .with("a", "3", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result,
                "a", "1"
        );

        assertUpdated(n1);
        assertUpdated(n2, "a", "1");
        assertUpdated(n3, "a", "1");
    }

    @Test
    void lesserValue2() {
        SimpleNode n1 = node("n1")
                .with("a", "22", 100);
        SimpleNode n2 = node("n2")
                .with("a", "2", 100);
        SimpleNode n3 = node("n3")
                .with("a", "222", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result,
                "a", "2"
        );

        assertUpdated(n1, "a", "2");
        assertUpdated(n2);
        assertUpdated(n3, "a", "2");
    }

    @Test
    void lesserValue3() {
        SimpleNode n1 = node("n1")
                .with("a", "333", 100);
        SimpleNode n2 = node("n2")
                .with("a", "33", 100);
        SimpleNode n3 = node("n3")
                .with("a", "3", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result,
                "a", "3"
        );

        assertUpdated(n1, "a", "3");
        assertUpdated(n2, "a", "3");
        assertUpdated(n3);
    }

    @Test
    void tombstone1() {
        SimpleNode n1 = node("n1")
                .with("a", null, 100);
        SimpleNode n2 = node("n2");
        SimpleNode n3 = node("n3");

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result);

        assertUpdated(n1);
        assertUpdated(n2, "a", null);
        assertUpdated(n3, "a", null);
    }

    @Test
    void tombstone2() {
        SimpleNode n1 = node("n1");
        SimpleNode n2 = node("n2")
                .with("a", null, 100);
        SimpleNode n3 = node("n3");

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result);

        assertUpdated(n1, "a", null);
        assertUpdated(n2);
        assertUpdated(n3, "a", null);
    }

    @Test
    void tombstone3() {
        SimpleNode n1 = node("n1");
        SimpleNode n2 = node("n2");
        SimpleNode n3 = node("n3")
                .with("a", null, 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result);

        assertUpdated(n1, "a", null);
        assertUpdated(n2, "a", null);
        assertUpdated(n3);
    }

    @Test
    void fresherTombstone1() {
        SimpleNode n1 = node("n1")
                .with("a", null, 100);
        SimpleNode n2 = node("n2")
                .with("a", "2", 100);
        SimpleNode n3 = node("n3")
                .with("a", "3", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result);

        assertUpdated(n1);
        assertUpdated(n2, "a", null);
        assertUpdated(n3, "a", null);
    }

    @Test
    void fresherTombstone2() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100);
        SimpleNode n2 = node("n2")
                .with("a", null, 100);
        SimpleNode n3 = node("n3")
                .with("a", "3", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result);

        assertUpdated(n1, "a", null);
        assertUpdated(n2);
        assertUpdated(n3, "a", null);
    }

    @Test
    void fresherTombstone3() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100);
        SimpleNode n2 = node("n2")
                .with("a", "2", 100);
        SimpleNode n3 = node("n3")
                .with("a", null, 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result);

        assertUpdated(n1, "a", null);
        assertUpdated(n2, "a", null);
        assertUpdated(n3);
    }
}
