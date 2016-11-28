package com.sgoeddel.wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordCountReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    //reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Map<Integer, Integer> occurrenceToDocNumber = new HashMap<>();
        StringBuilder outputString = new StringBuilder();
            /*iterates through all the values available with a key and add them together and give the
            final result as the key and sum of its values*/
        while (values.hasNext()) {
            String rawInput = values.next().toString();
            addOccurrenceToMap(occurrenceToDocNumber, getDocNumber(rawInput), getOccurrenceNumber(rawInput));
        }

        outputString.append(occurrenceToDocNumber.size()).append(" : ");

        for (Integer occurrence : occurrenceToDocNumber.keySet()) {
            outputString.append("(").append(occurrence.toString()).append(", ").append(occurrenceToDocNumber.get(occurrence)).append("), ");
        }
        String finalReverseIndicies = outputString.substring(0, outputString.length() - 2);
        Text reverseText = new Text();
        reverseText.set(finalReverseIndicies);
        output.collect(key, reverseText);
    }

    public int getOccurrenceNumber(String rawInput) {
        int comma = rawInput.indexOf(",");
        return Integer.parseInt(rawInput.substring(1, comma));
    }

    public int getDocNumber(String rawInput) {
        int comma = rawInput.indexOf(",");
        return Integer.parseInt(rawInput.substring(comma + 2, rawInput.length() - 1));
    }

    public void addOccurrenceToMap(Map<Integer, Integer> occurrenceToDocNumber, int documentNumber, int occurrenceTimes) {
        Integer occurrences = occurrenceToDocNumber.get(documentNumber);
        if (occurrences != null) {
            occurrenceToDocNumber.put(documentNumber, occurrences + 1);
        } else {
            occurrenceToDocNumber.put(documentNumber, 1);
        }
    }
}