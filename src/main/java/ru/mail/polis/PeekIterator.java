package ru.mail.polis;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class PeekIterator<R> implements Iterator<R> {

    private final Iterator<R> delegate;
    private R current;

    PeekIterator(Iterator<R> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext() || current != null;
    }

    R peek() {
        if (current != null)
            return current;

        if (!delegate.hasNext())
            return null;

        current = delegate.next();
        return current;
    }

    @Override
    public R next() {
        if (!hasNext())
            throw new NoSuchElementException();

        R prevPeek = peek();
        current = null;
        return prevPeek;

    }
}
