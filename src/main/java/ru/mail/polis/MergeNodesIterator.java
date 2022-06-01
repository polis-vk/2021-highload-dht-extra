package ru.mail.polis;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

final class MergeNodesIterator implements Iterator<Record> {

    private final Map<Node, PeekingIterator> nodesWithIterators;

    MergeNodesIterator(Map<Node, PeekingIterator> nodesWithIterators) {
        this.nodesWithIterators = nodesWithIterators;
    }

    @Override
    public boolean hasNext() {
        for (PeekingIterator iter : nodesWithIterators.values())
            if (iter.hasNext())
                return true;
        return false;
    }

    private static class NodeData {
        public final Node node;
        public Record record;
        public final Iterator<Record> iterator;

        public NodeData(Node node, Record record, Iterator<Record> iterator) {
            this.node = node;
            this.record = record;
            this.iterator = iterator;
        }
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No elements");
        }

        NodeData bestNodeData = null;

        // find best record in layer
        for (Map.Entry<Node, PeekingIterator> entry : nodesWithIterators.entrySet()) {
            NodeData currNodeData = new NodeData(entry.getKey(), entry.getValue().peek(), entry.getValue());

            if (!currNodeData.iterator.hasNext()) {
                continue;
            }

            if (bestNodeData == null) {
                bestNodeData = new NodeData(currNodeData.node, currNodeData.record, currNodeData.iterator);
                continue;
            }

            int compRes = bestNodeData.record.compareTo(currNodeData.record);
            if (compRes < 0) {
                bestNodeData = new NodeData(currNodeData.node, currNodeData.record, currNodeData.iterator);
            }

        }

        assert bestNodeData != null;

        // update others nodes
        for (Map.Entry<Node, PeekingIterator> entry : nodesWithIterators.entrySet()) {
            NodeData currNodeData = new NodeData(entry.getKey(), entry.getValue().peek(), entry.getValue());

            // pass iterators without elements
            if (!currNodeData.iterator.hasNext()) {
                continue;
            }

            assert currNodeData.record != null;
            int compareKeyResult = bestNodeData.record.key.compareTo(currNodeData.record.key);
            if (compareKeyResult < 0) {
                currNodeData.node.update(bestNodeData.record);
                continue;
            } else if (compareKeyResult > 0) {
                continue;
            }

            int compRes = bestNodeData.record.compareTo(currNodeData.record);
            if (compRes > 0) {
                currNodeData.node.update(bestNodeData.record);
                currNodeData.iterator.next();
            } else {
                currNodeData.iterator.next();
            }

        }
        return bestNodeData.record;
    }

}
