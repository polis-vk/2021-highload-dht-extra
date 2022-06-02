package ru.mail.polis;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class PeekIterator<R> implements Iterator<Record> {

    private final Iterator<Record> delegate;
    private Record current;

    PeekIterator(Iterator<Record> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext() || current != null;
    }

    Record peek() {
        if (current != null)
            return current;

        if (!delegate.hasNext())
            return null;

        current = delegate.next();
        return current;
    }

    @Override
    public Record next() {
        if (!hasNext())
            throw new NoSuchElementException();

        Record prevPeek = peek();
        current = null;
        return prevPeek;

    }
}
