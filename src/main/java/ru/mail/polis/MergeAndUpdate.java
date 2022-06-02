package ru.mail.polis;

import ru.mail.polis.iterators.PeekingIterator;
import ru.mail.polis.utils.CurrentNode;

import java.util.*;

public class MergeAndUpdate implements Iterator<Record> {

    private final Map<Node,PeekingIterator> nodesWithRecords;
    private final Set<Map.Entry<Node,PeekingIterator>> entries;

    public MergeAndUpdate(Map<Node, PeekingIterator> nodesWithRecords)
    {
        this.nodesWithRecords = nodesWithRecords;
        this.entries = this.nodesWithRecords.entrySet();
    }
    @Override
    public boolean hasNext() {
        for (PeekingIterator peekingIterator : nodesWithRecords.values())
            if (peekingIterator.hasNext())
                return true;
        return false;
    }
    @Override
    public Record next() {
        CurrentNode currentNode = null;
        for(Map.Entry<Node,PeekingIterator> entry : entries){
            if(!entry.getValue().hasNext())
            {
                continue;
            }
            if (currentNode == null )
            {
                currentNode = new CurrentNode(entry.getKey(), entry.getValue(), Objects.requireNonNull(entry.getValue().peek()));
            }
            //When a current record is null
            if (currentNode.getRecord() == null)
                continue;

            Record nextRecord = entry.getValue().peek();

            if (nextRecord!= null)
            {
                int compareKeys = currentNode.getRecord().key.compareTo(nextRecord.key);

                if(compareKeys > 0) //keys
                {
                    currentNode = new CurrentNode(entry.getKey(), entry.getValue(),nextRecord);
                }
                else if (compareKeys == 0)
                {
                    int compareTimeStamp = Long.compare(currentNode.getRecord().ts,nextRecord.ts); //ts
                    if (compareTimeStamp < 0){
                        currentNode = new CurrentNode(entry.getKey(), entry.getValue(),nextRecord);
                    }
                    else if (compareTimeStamp == 0){
                        if(!nextRecord.isTombstone() && !currentNode.getRecord().isTombstone())
                        {
                            int compareValues = currentNode.getRecord().value.compareTo(nextRecord.value);
                            if (compareValues > 0)
                            {
                                currentNode = new CurrentNode(entry.getKey(), entry.getValue(),nextRecord);
                            }
                        }
                        else if(! currentNode.getRecord().isTombstone()){
                            currentNode = new CurrentNode(entry.getKey(), entry.getValue(),nextRecord);
                        }
                    }
                }
            }
        }
        //---------UPDATE NODES-----------
        updateNodes(currentNode);

        assert currentNode != null;
        return currentNode.getRecord();
    }

    public void updateNodes(CurrentNode dataNode){

        for (Map.Entry<Node, PeekingIterator> entry : entries) {
            CurrentNode currentNode = new CurrentNode(entry.getKey(), entry.getValue(),entry.getValue().peek());

            if (!entry.getValue().hasNext()) {
                currentNode.getNode().update(dataNode.getRecord());
                continue;
            }
           //When a current record is null
            if (currentNode.getRecord() == null) {
                currentNode.getNode().update(dataNode.getRecord());
                continue;
            }
            int compareKeyResult = dataNode.getRecord().key.compareTo(currentNode.getRecord().key);
            if (compareKeyResult < 0) {
                currentNode.getNode().update(dataNode.getRecord());
            }
            else if (compareKeyResult == 0) {
                int compareTimeStamp = Long.compare(currentNode.getRecord().ts,dataNode.getRecord().ts); //ts

                if (compareTimeStamp < 0){
                    currentNode.getNode().update(dataNode.getRecord());
                    currentNode.getIterator().next();
                }
                 else if (compareTimeStamp == 0) {
                        if (!currentNode.getRecord().isTombstone()) {
                            if (dataNode.getRecord().isTombstone()) {
                                currentNode.getNode().update(dataNode.getRecord());
                                currentNode.getIterator().next();
                            } else {
                                int compareValueResult = dataNode.getRecord().value.compareTo(currentNode.getRecord().value);
                                if (compareValueResult == 0) {
                                    currentNode.getIterator().next();
                                } else if (compareValueResult < 0) {
                                    currentNode.getNode().update(dataNode.getRecord());
                                    currentNode.getIterator().next();
                                }
                            }
                        } else {
                            if (dataNode.getRecord().isTombstone()) {
                                currentNode.getIterator().next();
                            }
                        }
                    }
                }
            }
        }
    }