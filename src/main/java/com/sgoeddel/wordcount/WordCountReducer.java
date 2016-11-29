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
        Map<String, Integer> occurrencesAndIndices = new HashMap<>();
        StringBuilder outputString = new StringBuilder();
            /*iterates through all the values available with a key and add them together and give the
            final result as the key and sum of its values*/
        while (values.hasNext()) {
            String rawInput = values.next().toString();
            addOccurrenceToMap(occurrencesAndIndices, getDocNumberAndChapter(rawInput), getOccurrenceNumber(rawInput));
        }

        outputString.append(occurrencesAndIndices.size()).append(" : ");

        for (String index : occurrencesAndIndices.keySet()) {
            outputString.append("(").append(index).append(", ").append(occurrencesAndIndices.get(index)).append("), ");
        }
        String finalReverseIndicies = outputString.substring(0, outputString.length() - 2);
        Text reverseText = new Text();
        reverseText.set(finalReverseIndicies);
        output.collect(key, reverseText);
    }

    private int getOccurrenceNumber(String rawInput) {
        int comma = rawInput.indexOf(",");
        return Integer.parseInt(rawInput.substring(1, comma));
    }

    private String getDocNumberAndChapter(String rawInput) {
        int comma = rawInput.indexOf(",");
        return rawInput.substring(comma + 2, rawInput.length() - 1);
    }

    private void addOccurrenceToMap(Map<String, Integer> occurrencesAndIndices, String documentNumberAndChapter, int occurrenceTimes) {
        Integer occurrences = occurrencesAndIndices.get(documentNumberAndChapter);
        if (occurrences != null) {
            occurrencesAndIndices.put(documentNumberAndChapter, occurrences + occurrenceTimes);
        } else {
            occurrencesAndIndices.put(documentNumberAndChapter, occurrenceTimes);
        }
    }
}