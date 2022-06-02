package ru.mail.polis;

import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class SingleCellCluster1Test extends TestBase {
    @Test
    void value() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1));
        assertKeyValues(result,
                "a", "1"
        );

        assertUpdated(n1);
    }

    @Test
    void tombstone() {
        SimpleNode n1 = node("n1")
                .with("a", null, 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1));
        assertKeyValues(result);

        assertUpdated(n1);
    }
}
