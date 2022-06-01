package ru.mail.polis;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class TombstonesFilterIterator implements Iterator<Record> {

    private Record aliveRecord;
    private final MergeNodesIterator mergeNodesIterator;

    TombstonesFilterIterator(MergeNodesIterator mergeNodesIterator) {
        this.mergeNodesIterator = mergeNodesIterator;
        peekNextAlive();
    }

    @Override
    public boolean hasNext() {
        return aliveRecord != null;
    }

    @Override
    public Record next() {
        if (!hasNext())
            throw new NoSuchElementException();

        Record prevAlive = aliveRecord;
        peekNextAlive();
        return prevAlive;
    }

    public void peekNextAlive() {
        while (mergeNodesIterator.hasNext()) {
            aliveRecord = mergeNodesIterator.next();
            if (!aliveRecord.isTombstone())
                return;
        }
        aliveRecord = null;
    }

}
