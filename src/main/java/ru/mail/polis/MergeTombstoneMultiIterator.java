package ru.mail.polis;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class MergeTombstoneMultiIterator implements Iterator<Record> {

    private Record current;
    private final MergeMultiIterator mergeMultiIterator;

    MergeTombstoneMultiIterator(MergeMultiIterator mergeMultiIterator) {
        this.mergeMultiIterator = mergeMultiIterator;
    }

    @Override
    public boolean hasNext() {
       if (current == null && !mergeMultiIterator.hasNext()) {
           return false;
       }
       Record nextR = peek();
       return nextR != null;
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Record now = peek();
        current = null;
        return now;
    }

    public Record peek() {
        if (current != null) {
            return current;
        }

        if (!mergeMultiIterator.hasNext()) {
            return null;
        }

        while (mergeMultiIterator.hasNext()) {
            current = mergeMultiIterator.next();
            if (!current.isTombstone()) {
                return current;
            }
        }
        return null;
    }
}
