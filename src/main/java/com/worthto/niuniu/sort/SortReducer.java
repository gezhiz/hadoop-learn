package com.worthto.niuniu.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 利用MapReduce的排序，对key（PageCount）进行排序
 * @author gezz
 * @description todo
 * @date 2019/9/27.
 */
public class SortReducer extends Reducer<PageCount,Void,PageCount,IntWritable> {
    private static IntWritable ONE = new IntWritable(1);

    @Override
    protected void reduce(PageCount key, Iterable<Void> values, Context context) throws IOException, InterruptedException {
        context.write(key, ONE);
    }
}
