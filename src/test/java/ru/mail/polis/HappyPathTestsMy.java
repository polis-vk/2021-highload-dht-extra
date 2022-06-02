package ru.mail.polis;

import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static ru.mail.polis.TestBase.*;

public class HappyPathTestsMy {
    @Test
    void myTestResultsConsistency() {
        for (int i = 0; i < 4; i++) {
            TestBase.SimpleNode n1 = node("n1")
                    .with("a", "1", 100)
                    .with("b", "1", 100)
                    .with("c", "1", 100)
                    .with("e", "2", 100);

            TestBase.SimpleNode n2 = node("n2")
                    .with("b", "2", 101)
                    .with("c", null, 101)
                    .with("d", "1", 100)
                    .with("e", "1", 100);

            TestBase.SimpleNode n3 = node("n2")
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
