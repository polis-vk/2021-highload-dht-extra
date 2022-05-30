package ru.mail.polis;

import java.util.*;

public final class RepairingMerger {
    /**
     * Merges <b>sorted</b> streams of {@link Record}s from cluster {@link Node}s and
     * <b>repairs</b> discrepancies using {@link Node#update(Record)} facility.
     * <p>
     * The priority of {@link Record}s:
     * <ol>
     *     <li>key is lesser</li>
     *     <li>timestamp is greater</li>
     *     <li>tombstone</li>
     *     <li>value is lesser</li>
     * </ol>
     *
     * @param nodesWithIterators mapping of {@link Node}s to corresponding {@link Record} streams
     * @return sorted stream of the freshest {@link Record} <b>without tombstones</b>
     */
    public static Iterator<Record> mergeAndRepair(Map<Node, Iterator<Record>> nodesWithIterators) {
        Map<String, Record> keyRecordMap = new HashMap<>();
        for (Map.Entry<Node, Iterator<Record>> nodeRecords1 : nodesWithIterators.entrySet()
        ) {
            for (Map.Entry<Node, Iterator<Record>> nodeRecords2 : nodesWithIterators.entrySet()
            ) {
                if (nodeRecords1.getKey() == nodeRecords2.getKey())
                    continue;

                for (Iterator<Record> it1 = nodeRecords1.getValue(); it1.hasNext(); ) {
                    Record addedRecord = it1.next();
                    if (keyRecordMap.keySet().contains(addedRecord.key))
                        continue;
                    for (Iterator<Record> it2 = nodeRecords2.getValue(); it2.hasNext(); ) {
                        Record checkedRecord = it2.next();
                        //------------------------ KEY COMPARISON ------------------------------------------------------
                        int keyComp = addedRecord.key.compareTo(checkedRecord.key);
                        if (keyComp < 0) {
                            addedRecord = checkedRecord;
                            //TODO synchronize
                            break;
                        } else if (keyComp > 0) {
                            continue;
                        }

                        //------------------------ TS COMPARISON -------------------------------------------------------
                        int tsComp = Long.compare(addedRecord.ts, checkedRecord.ts);
                        if (tsComp < 0) {
                            addedRecord = checkedRecord;
                            //TODO synchronize
                            break;
                        } else if (tsComp > 0) {
                            continue;
                        }

                        //------------------------ TOMBSTONES ----------------------------------------------------------
                        if (addedRecord.value == null && checkedRecord.value != null)
                            break;
                        else if (addedRecord.value != null && checkedRecord.value == null) {
                            addedRecord = checkedRecord;
                            break;
                        } else if (addedRecord.value == null && checkedRecord.value == null) {
                            break;
                        }


                        //------------------------ VALUE COMPARISON ----------------------------------------------------
                        int valueComp = addedRecord.value.compareTo(checkedRecord.value);
                        if (valueComp > 0) {
                            addedRecord = checkedRecord;
                            //TODO synchronize
                            break;
                        } else if (valueComp < 0) {
                            continue;
                        }

                    }
                    keyRecordMap.put(addedRecord.key, addedRecord);

                }

            }
        }
        List<Record> finalList = new ArrayList<>();
        for (Map.Entry<String, Record> entry : keyRecordMap.entrySet()) {
            if (entry.getValue().value != null)
                finalList.add(entry.getValue());

        }
        return finalList.iterator();
    }
}
