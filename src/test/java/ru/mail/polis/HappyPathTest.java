package ru.mail.polis;

import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class HappyPathTest extends TestBase {
    @Test
    void single() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100);
        SimpleNode n2 = node("n2")
                .with("a", "1", 100);
        SimpleNode n3 = node("n3")
                .with("a", "1", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result,
                "a", "1"
        );

        // Nothing updated
        assertUpdated(n1);
        assertUpdated(n2);
        assertUpdated(n3);
    }

    @Test
    void singleTombstone() {
        SimpleNode n1 = node("n1")
                .with("a", null, 100);
        SimpleNode n2 = node("n2")
                .with("a", null, 100);
        SimpleNode n3 = node("n3")
                .with("a", null, 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result);

        // Nothing updated
        assertUpdated(n1);
        assertUpdated(n2);
        assertUpdated(n3);
    }

    @Test
    void latestWins() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100);
        SimpleNode n2 = node("n2")
                .with("a", "2", 102);
        SimpleNode n3 = node("n3")
                .with("a", "3", 101);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result,
                "a", "2"
        );

        // Nothing updated
        assertUpdated(n1, "a", "2");
        assertUpdated(n2);
        assertUpdated(n3, "a", "2");
    }

    @Test
    void tombstoneWins() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100);
        SimpleNode n2 = node("n2")
                .with("a", null, 100);
        SimpleNode n3 = node("n3")
                .with("a", "2", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result);

        // Nothing updated
        assertUpdated(n1, "a", null);
        assertUpdated(n2);
        assertUpdated(n3, "a", null);
    }

    @Test
    void smallerValueWins() {
        SimpleNode n1 = node("n1")
                .with("a", "2", 100);
        SimpleNode n2 = node("n2")
                .with("a", "1", 100);
        SimpleNode n3 = node("n3")
                .with("a", "3", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2, n3));
        assertKeyValues(result, "a", "1");

        // Nothing updated
        assertUpdated(n1, "a", "1");
        assertUpdated(n2);
        assertUpdated(n3, "a", "1");
    }

    @Test
    void mix() {
        SimpleNode n1 = node("n1")
                .with("a", "1", 100)
                .with("b", "1", 100)
                .with("c", "1", 100)
                .with("e", "2", 100);

        SimpleNode n2 = node("n2")
                .with("b", "2", 101)
                .with("c", null, 100)
                .with("d", "1", 100)
                .with("e", "1", 100);

        Iterator<Record> result = RepairingMerger.mergeAndRepair(iterators(n1, n2));
        assertKeyValues(result,
                "a", "1",
                "b", "2",
                "d", "1",
                "e", "1"
        );

        assertUpdated(n1,
                "b", "2",
                "c", null,
                "d", "1",
                "e", "1"
        );

        assertUpdated(n2,
                "a", "1"
        );
    }

    @Test
    void myTestResultsConsistency() {
        for (int i = 0; i < 4; i++) {
            SimpleNode n1 = node("n1")
                    .with("a", "1", 100)
                    .with("b", "1", 100)
                    .with("c", "1", 100)
                    .with("e", "2", 100);

            SimpleNode n2 = node("n2")
                    .with("b", "2", 101)
                    .with("c", null, 101)
                    .with("d", "1", 100)
                    .with("e", "1", 100);

            SimpleNode n3 = node("n2")
                    .with("b", "2", 101)
                    .with("c", "2", 101)
                    .with("d", "2", 101)
                    .with("e", "1", 100);

            Iterator<Record> result = i % 2 == 0 ?
                    RepairingMerger.mergeAndRepair(iterators(n1, n2, n3))
                    :
                    RepairingMerger.mergeAndRepair(iterators(n2, n1, n3));

            assertKeyValues(result,
                    "a", "1",
                    "b", "2",
                    "d", "2",
                    "e", "1"
            );

            assertUpdated(n1,
                    "b", "2",
                    "c", null,
                    "d", "2",
                    "e", "1"
            );

            assertUpdated(n2,
                    "a", "1",
                    "d", "2"
            );

            assertUpdated(n3,
                    "a", "1",
                    "c", null
            );
        }

    }
}
