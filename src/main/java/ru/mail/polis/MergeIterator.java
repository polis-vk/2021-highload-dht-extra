package ru.mail.polis;

import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;

public class MergeIterator implements Iterator<Record> {
    private final Map<Node, PeekIterator<Record>> peekIteratorsMap;

    public static class NodeData {
        private final Node node;
        private final PeekIterator<Record> iterator;
        private final Record record;

        NodeData(Node node, PeekIterator<Record> iterator, Record record) {
            this.node = node;
            this.iterator = iterator;
            this.record = record;
        }
    }


    MergeIterator(Map<Node, PeekIterator<Record>> peekIterators) {
        this.peekIteratorsMap = peekIterators;
    }

    @Override
    public boolean hasNext() {
        for (PeekIterator<Record> peekIterator : peekIteratorsMap.values())
            if (peekIterator.hasNext())
                return true;
        return false;
    }

    @Override
    public Record next() {
        List<NodeData> currLayer = new LinkedList<>();
        NodeData bestData = null;
        for (Map.Entry<Node, PeekIterator<Record>> entry : peekIteratorsMap.entrySet()) {
            if (!entry.getValue().hasNext())
                continue;

            NodeData currData = new NodeData(entry.getKey(), entry.getValue(), entry.getValue().peek());
            currLayer.add(currData);

            if (bestData == null)
                bestData = currData;

            int compRes = bestData.record.compareTo(currData.record);
            if (compRes < 0)
                bestData = currData;

        }

        assert bestData != null;

        for (NodeData currData : currLayer) {
            int compKey = bestData.record.key.compareTo(currData.record.key);
            if (compKey < 0) {
                currData.node.update(bestData.record);
                continue;
            } else if (compKey > 0) {
                currData.iterator.next();
                continue;
            }

            int compRes = bestData.record.compareTo(currData.record);
            if (compRes > 0) {
                currData.node.update(bestData.record);
                currData.iterator.next();
            } else {
                currData.iterator.next();
            }

        }

//        StringBuilder layerStr = new StringBuilder();
//        for (NodeData nodeData : currLayer)
//            layerStr.append(nodeData.record);
//        System.out.println("layer = " + layerStr);
//        System.out.println("best = " + bestData.record);
        return bestData.record;
    }
}
