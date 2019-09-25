package com.worthto.niuniu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author gezz
 * @description todo
 * @date 2019/9/25.
 */

public class WordCountReduce extends Reducer<Text,LongWritable,Text,LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Long sum = 0L;
        for(LongWritable value: values) {
            sum += value.get();
        }
        context.write(key, new LongWritable(sum));
    }
}