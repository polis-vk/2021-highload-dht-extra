package ru.mail.polis;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

final class MergeMultiIterator implements Iterator<Record> {

    private final Map<Node, PeekingIterator> nodesWithIterators;

    private boolean hasNextCheck;

    MergeMultiIterator(Map<Node, PeekingIterator> nodesWithIterators) {
        this.nodesWithIterators = nodesWithIterators;
        hasNextCheck = true;
    }

    @Override
    public boolean hasNext() {
        return hasNextCheck;
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No elements");
        }

        CurrentData currentData = null;

        // Assign current best
        for (Map.Entry<Node, PeekingIterator> entry : nodesWithIterators.entrySet()) {
            if (!entry.getValue().hasNext()) {
                continue;
            }

            if (currentData == null) {
                Record record = Objects.requireNonNull(entry.getValue().peek());
                currentData = new CurrentData(entry.getKey(), record, entry.getValue());
            } else {
                Record recordNext = Objects.requireNonNull(entry.getValue().peek());

                int compareKeyResult = currentData.record.key.compareTo(recordNext.key);
                if (compareKeyResult == 0) {
                    if (currentData.record.ts == recordNext.ts) {
                        if (!recordNext.isTombstone()) {
                            if (!currentData.record.isTombstone()) {
                                int compareValueResult = currentData.record.value.compareTo(recordNext.value);
                                if (compareValueResult > 0) {
                                    currentData = new CurrentData(entry.getKey(), recordNext, entry.getValue());
                                }
                            }
                        } else if (!currentData.record.isTombstone()){
                            currentData = new CurrentData(entry.getKey(), recordNext, entry.getValue());
                        }
                    } else if (currentData.record.ts < recordNext.ts) {
                        currentData = new CurrentData(entry.getKey(), recordNext, entry.getValue());
                    }
                } else if (compareKeyResult > 0) {
                    currentData = new CurrentData(entry.getKey(), recordNext, entry.getValue());
                }
            }
        }

        Objects.requireNonNull(currentData);

        // Update others
        hasNextCheck = false;
        for (Map.Entry<Node, PeekingIterator> entry : nodesWithIterators.entrySet()) {
            if (!entry.getValue().hasNext()) {
                continue;
            }

            Record recordNext = Objects.requireNonNull(entry.getValue().peek());
            int compareKeyResult = currentData.record.key.compareTo(recordNext.key);
            if (compareKeyResult == 0) {
                if (currentData.record.ts == recordNext.ts) {
                    if (!recordNext.isTombstone()) {
                        if (currentData.record.isTombstone()) {
                            updateNode(entry.getKey(), currentData.record);
                            entry.getValue().next();
                        } else {
                            int compareValueResult = currentData.record.value.compareTo(recordNext.value);
                            if (compareValueResult == 0) {
                                entry.getValue().next();
                            } else if (compareValueResult < 0) {
                                updateNode(entry.getKey(), currentData.record);
                                entry.getValue().next();
                            }
                        }
                    } else {
                        if (currentData.record.isTombstone()) {
                            entry.getValue().next();
                        }
                    }
                } else if (currentData.record.ts > recordNext.ts) {
                    updateNode(entry.getKey(), currentData.record);
                    entry.getValue().next();
                }
            } else if (compareKeyResult < 0) {
                updateNode(entry.getKey(), currentData.record);
            }

            if (entry.getValue().hasNext()) {
                hasNextCheck = true;
            }
        }

        return currentData.record;
    }

    private void updateNode(Node node, Record record) {
        node.update(record);
    }

    private static class CurrentData {
        public final Node node;
        public Record record;
        public final Iterator<Record> iterator;

        public CurrentData(Node node, Record record, Iterator<Record> iterator) {
            this.node = node;
            this.record = record;
            this.iterator = iterator;
        }
    }
}
