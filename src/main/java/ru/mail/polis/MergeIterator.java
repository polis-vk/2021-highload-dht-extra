package ru.mail.polis;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;

public class MergeIterator implements Iterator<Record> {
    private final Map<Node, PeekIterator<Record>> peekIteratorsMap;
    private final Comparator<Record> recordComparator;

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


    MergeIterator(Map<Node, PeekIterator<Record>> peekIterators, Comparator<Record> recordComparator) {
        this.peekIteratorsMap = peekIterators;
        this.recordComparator = recordComparator;
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
            NodeData currData = new NodeData(entry.getKey(), entry.getValue(), entry.getValue().peek());
            currLayer.add(currData);

            if (currData.record == null)
                continue;

            if (bestData == null)
                bestData = currData;

            int compRes = recordComparator.compare(bestData.record, currData.record);
            if (compRes < 0)
                bestData = currData;

        }

        assert bestData != null;

        for (NodeData currData : currLayer) {
            if (currData.record == null) {
                currData.node.update(bestData.record);
                continue;
            }

            int compKey = bestData.record.key.compareTo(currData.record.key);
            assert compKey <= 0;//logic bugs catching
            if (compKey < 0) {
                //missed key in another node
                currData.node.update(bestData.record);
                continue;
            }

            int compRes = recordComparator.compare(bestData.record, currData.record);
            assert compRes >= 0;//logic bugs catching
            if (compRes > 0) {
                //overwriting record in another node
                currData.node.update(bestData.record);
            }
            currData.iterator.next();

        }

//        StringBuilder layerStr = new StringBuilder();
//        for (NodeData nodeData : currLayer)
//            layerStr.append(nodeData.record);
//        System.out.println("layer = " + layerStr);
//        System.out.println("best = " + bestData.record);
        return bestData.record;
    }
}
