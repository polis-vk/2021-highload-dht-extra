package ru.mail.polis.iterators;

import ru.mail.polis.Record;

import java.util.Iterator;

public class PeekingIterator implements Iterator<Record> {

    private Iterator<Record> records;
    private Record nextValue;

    public PeekingIterator(Iterator<Record> records){
        this.records = records;
        this.nextValue = this.records.hasNext()? this.records.next() : null;
    }
    @Override
    public boolean hasNext() {
        return (nextValue != null);
    }

    @Override
    public Record next() {
        Record oldValue = peek();
        nextValue = this.records.hasNext()? this.records.next() : null;
        return oldValue;
    }

    public Record peek(){
        return nextValue;

    }
}
