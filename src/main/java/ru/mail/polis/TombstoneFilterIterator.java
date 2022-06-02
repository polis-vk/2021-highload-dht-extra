package ru.mail.polis;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class TombstoneFilterIterator implements Iterator<Record> {

    private final MergeIterator mergeIterator;
    private Record currAlive;

    TombstoneFilterIterator(MergeIterator mergeIterator) {
        this.mergeIterator = mergeIterator;
        this.currAlive = peekNextAlive();
    }

    @Override
    public boolean hasNext() {
        return currAlive != null && !currAlive.isTombstone();
    }

    @Override
    public Record next() {
        if (!hasNext())
            throw new NoSuchElementException();
        Record prevAlive = currAlive;
        currAlive = peekNextAlive();
        return prevAlive;
    }

    Record peekNextAlive() {
        Record nextAlive;
        while (mergeIterator.hasNext()) {
            nextAlive = mergeIterator.next();
            if (!nextAlive.isTombstone())
                return nextAlive;
        }
        return null;
    }
}
