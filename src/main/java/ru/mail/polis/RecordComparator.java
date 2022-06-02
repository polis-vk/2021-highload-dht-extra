package ru.mail.polis;

import java.util.Comparator;

public class RecordComparator implements Comparator<Record> {
    @Override
    public int compare(Record rec1, Record rec2) {
        int compareKey = rec1.key.compareTo(rec2.key);
        if (compareKey != 0)
            return -compareKey;

        int compareTs = Long.compare(rec1.ts, rec2.ts);
        if (compareTs != 0)
            return compareTs;

        if (rec1.isTombstone() && rec2.isTombstone())
            return 0;
        else if (rec1.isTombstone())
            return 1;
        else if (rec2.isTombstone())
            return -1;

        int compareVal = rec1.value.compareTo(rec2.value);
        if (compareVal != 0)
            return -compareVal;

        return 0;


    }
}
