package ru.mail.polis;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class TombstoneFilterIterator implements Iterator<Record> {

    private final Iterator<Record> delegate;
    private Record currAlive;

    TombstoneFilterIterator(MergeIterator mergeIterator) {
        this.delegate = mergeIterator;
        this.currAlive = peekNextAlive();
    }

    @Override
    public boolean hasNext() {
        return currAlive != null;
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
        while (delegate.hasNext()) {
            nextAlive = delegate.next();
            if (!nextAlive.isTombstone())
                return nextAlive;
        }
        return null;
    }
}
