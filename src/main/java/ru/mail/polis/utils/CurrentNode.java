package ru.mail.polis.utils;

import ru.mail.polis.Node;
import ru.mail.polis.Record;
import ru.mail.polis.iterators.PeekingIterator;

public class CurrentNode {

    private final Node node;
    private final PeekingIterator iterator;
    private final Record record;

    public CurrentNode(Node node, PeekingIterator iterator, Record record) {
        this.node = node;
        this.iterator = iterator;
        this.record = record;
    }

    public Node getNode() {
        return node;
    }

    public PeekingIterator getIterator() {
        return iterator;
    }

    public Record getRecord() {
        return record;
    }


}

