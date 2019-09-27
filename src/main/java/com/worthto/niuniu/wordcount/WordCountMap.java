package com.worthto.niuniu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author gezz
 * @description todo
 * @date 2019/9/25.
 */
public class WordCountMap extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final static LongWritable ONE = new LongWritable(1);
    private Text word = new Text();

    /**
     *
     * @param key 每个数据的记录在数据分片中的字节偏移量，数据类型是LongWritable
     * @param value 每行的内容
     * @throws IOException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            try {
                context.write(word, ONE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}