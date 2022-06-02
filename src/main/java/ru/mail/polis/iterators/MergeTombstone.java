package ru.mail.polis.iterators;

import ru.mail.polis.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class MergeTombstone implements Iterator<Record> {
    private final PeekingIterator peekingIterator;

    public MergeTombstone(Iterator<Record> recordIterator){
        this.peekingIterator =  new PeekingIterator(recordIterator);
    }
    @Override
    public boolean hasNext() {
        for(;;){
            Record peek = this.peekingIterator.peek();
            if(peek == null){
                return false;
            }
            if(!peek.isTombstone()){
                return true;
            }
            this.peekingIterator.next();
        }
    }

    @Override
    public Record next() {
        if (!hasNext()){
            throw new NoSuchElementException("No elements");
        }
        return this.peekingIterator.next();

    }
}
