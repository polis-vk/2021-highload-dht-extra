package ru.mail.polis;

public class Record implements Comparable<Record> {

    public final Node node;
    public final String key;
    public final String value;
    public final long ts;

    public Record(Node node, String key, String value, long ts) {
        this.node = node;
        this.key = key;
        this.value = value;
        this.ts = ts;
    }

    public boolean isTombstone() {
        return value == null;
    }

    @Override
    public String toString() {
        return "Record{" +
                "node=" + node +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", ts=" + ts +
                '}';
    }

    @Override
    public int compareTo(Record record) {
        int compareKey = this.key.compareTo(record.key);
        if (compareKey != 0)
            return -compareKey;

        int compareTs = Long.compare(this.ts, record.ts);
        if (compareTs != 0)
            return compareTs;

        if (this.isTombstone() && record.isTombstone())
            return 0;
        else if (this.isTombstone())
            return 1;
        else if (record.isTombstone())
            return -1;

        int compareVal = this.value.compareTo(record.value);
        if (compareVal != 0)
            return -compareVal;

        return 0;

    }
}
