package ru.mail.polis;

import ru.mail.polis.iterators.PeekingIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

public class MergeAndUpdate implements Iterator<Record> {

    private Map<Node,PeekingIterator> nodesWithRecords;
    private Record finalExpectedRecord;
    private boolean validate ;

    public MergeAndUpdate(Map<Node, PeekingIterator> nodesWithRecords)
    {
        this.nodesWithRecords = nodesWithRecords;
        for (PeekingIterator peekingIterator : nodesWithRecords.values()){
            if (peekingIterator.hasNext()) {
                validate = true;
                break;
            }
        }
    }
    @Override
    public boolean hasNext() {
        return validate;
    }
    @Override
    public Record next() {
        Record currentRecord;
        //-------------MERGE NODES----------
        for(Map.Entry<Node,PeekingIterator> entry : nodesWithRecords.entrySet()){
            if(!hasNext())
            {
               throw new NoSuchElementException("No elements founds");
            }
            if(this.finalExpectedRecord == null)
            {
                currentRecord = entry.getValue().peek();
                this.finalExpectedRecord = currentRecord;
            }
            else
            {
                Record nextRecord = entry.getValue().peek();
                //COMPARISON
                int compareKeys = this.finalExpectedRecord.key.compareTo(nextRecord.key);
                if(compareKeys > 0) //keys
                {
                    this.finalExpectedRecord = nextRecord;
                }
                else if (compareKeys == 0)
                {
                    int compareTimeStamp = Long.compare(this.finalExpectedRecord.ts,nextRecord.ts); //ts
                    if (compareTimeStamp < 0){
                        this.finalExpectedRecord = nextRecord;
                    }
                    else if (compareTimeStamp == 0){
                        if(!nextRecord.isTombstone() && !this.finalExpectedRecord.isTombstone())
                        {
                            int compareValues = this.finalExpectedRecord.value.compareTo(nextRecord.value);
                            if (compareValues > 0)
                            {
                                this.finalExpectedRecord = nextRecord;
                            }
                        }
                        else if(!this.finalExpectedRecord.isTombstone()){
                            this.finalExpectedRecord = nextRecord;
                        }
                    }
                }
            }
        }
        validate = false;
        //---------UPDATE NODES-----------
        for(Map.Entry<Node,PeekingIterator> entry : nodesWithRecords.entrySet()){

            if(!entry.getValue().hasNext())
            {
                continue;
            }
            Record currRecord = Objects.requireNonNull(entry.getValue().peek());
            int compareKeys = this.finalExpectedRecord.key.compareTo(currRecord.key); // keys
            if(compareKeys<0){
                entry.getKey().update(this.finalExpectedRecord);
            }
            int compareTimeStamp = Long.compare(this.finalExpectedRecord.ts,currRecord.ts); //ts
            if (compareTimeStamp > 0){
                    entry.getKey().update(this.finalExpectedRecord);
                    //entry.getValue().next();
            }
            else if(compareTimeStamp == 0)
                {
                    if(!currRecord.isTombstone()){
                        if (this.finalExpectedRecord.isTombstone()){
                           entry.getKey().update(this.finalExpectedRecord);
                           entry.getValue().next();
                        }
                        else{
                            int compareValues = this.finalExpectedRecord.value.compareTo(currRecord.value);//values
                            if (compareValues <0){
                                entry.getKey().update(this.finalExpectedRecord);
                                entry.getValue().next();
                            }
                        }
                    }

                }
            }
        //}
        return Objects.requireNonNull(this.finalExpectedRecord);
    }
}
