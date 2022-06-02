package ru.mail.polis;

public class Record  {

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


}
