package com.sgoeddel.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private Text occurrence = new Text();

    //map method that performs the tokenizer job and framing the initial key value pairs
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        //taking one line at a time and tokenizing the same
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        //iterating through all the words available in that line and forming the key value pair
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            String time = "1";
            String indexAndCount = "(" + time + ", " + key + ")";
            occurrence.set(indexAndCount);
            //sending to output collector which in turn passes the same to reducer
            output.collect(word, occurrence);
        }
    }

//    @Override
//    public void configure(JobConf job) {
//        //TODO: figure out how to get the line number
//        job.getInputFormat()
//    }
}