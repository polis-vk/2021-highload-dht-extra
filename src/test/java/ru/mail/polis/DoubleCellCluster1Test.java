package ru.mail.polis;

import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class DoubleCellCluster1Test extends TestBase {
    @Test
    void values() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100)
                .with("b", "2", 101);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1));
        assertKeyValues(result,
                "a", "1",
                "b", "2"
        );

        assertUpdated(n1);
    }

    @Test
    void tombstones() {
        SimpleNode n1 = node("n1")
                .with("a", null, 100)
                .with("b", null, 101);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1));
        assertKeyValues(result);

        assertUpdated(n1);
    }

    @Test
    void valueTombstone() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100)
                .with("b", null, 101);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1));
        assertKeyValues(result,
                "a", "1"
        );

        assertUpdated(n1);
    }

    @Test
    void tombstoneValue() {
        SimpleNode n1 = node("n1")
                .with("a", null, 100)
                .with("b", "2", 101);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1));
        assertKeyValues(result,
                "b", "2"
        );

        assertUpdated(n1);
    }
}
