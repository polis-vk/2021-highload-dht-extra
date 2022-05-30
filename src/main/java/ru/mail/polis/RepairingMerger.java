package ru.mail.polis;

import java.util.*;

public final class RepairingMerger {

    public static void updateBestMap(Map<String, Record> bestRecordsMap, Record prevBest, Record candidate) {
        Record newBestRecord = new Record(
                prevBest.node,
                candidate.key, candidate.value, candidate.ts
        );
        bestRecordsMap.put(newBestRecord.key, newBestRecord);
    }

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
        Map<String, Record> bestRecordsMap = new HashMap<>();

        while (true) {

            Map<Node, Record> currLayer = new HashMap<>();
            for (Map.Entry<Node, Iterator<Record>> nodeIteratorEntry : nodesWithIterators.entrySet()) {
                Iterator<Record> it = nodeIteratorEntry.getValue();
                if (it.hasNext())
                    currLayer.put(nodeIteratorEntry.getKey(), it.next());
            }
            if (currLayer.size() == 0)
                break;

            for (Record candidateRecord : currLayer.values()) {
                if (!bestRecordsMap.keySet().contains(candidateRecord.key)) {
                    bestRecordsMap.put(candidateRecord.key, candidateRecord);
                    continue;
                }

                Record bestRecord = bestRecordsMap.get(candidateRecord.key);

                if (bestRecord == candidateRecord)
                    break;

                //------------------------ KEY COMPARISON ------------------------------------------------------
                int keyComp = bestRecord.key.compareTo(candidateRecord.key);
                if (keyComp > 0) {
                    updateBestMap(bestRecordsMap, bestRecord, candidateRecord);
                } else if (keyComp < 0) {
                    continue;
                }

                //------------------------ TS COMPARISON -------------------------------------------------------
                int tsComp = Long.compare(bestRecord.ts, candidateRecord.ts);
                if (tsComp < 0) {
                    updateBestMap(bestRecordsMap, bestRecord, candidateRecord);
                } else if (tsComp > 0) {
                    continue;
                }

                //------------------------ TOMBSTONES ----------------------------------------------------------
                if (bestRecord.isTombstone() && !candidateRecord.isTombstone())
                    continue;
                else if (!bestRecord.isTombstone() && candidateRecord.isTombstone()) {
                    updateBestMap(bestRecordsMap, bestRecord, candidateRecord);
                    continue;
                } else if (bestRecord.isTombstone() && candidateRecord.isTombstone()) {
                    continue;
                }


                //------------------------ VALUE COMPARISON ----------------------------------------------------
                int valueComp = bestRecord.value.compareTo(candidateRecord.value);
                if (valueComp > 0) {
                    updateBestMap(bestRecordsMap, bestRecord, candidateRecord);
                } else if (tsComp < 0) {
                    continue;
                }
            }
        }

        List<Record> bestRecords = new ArrayList<>();
        for (Map.Entry<String, Record> entry : bestRecordsMap.entrySet()) {
//            Record record = entry.getValue();
//            record.node.update(record);
            if (entry.getValue().value != null)
                bestRecords.add(entry.getValue());

        }
        return bestRecords.iterator();
    }
}
