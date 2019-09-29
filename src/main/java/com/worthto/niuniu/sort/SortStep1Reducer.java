package com.worthto.niuniu.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 接口访问统计Reducer，中间结果交给另外一个MapReduce处理
 * @author gezz
 * @description todo
 * @date 2019/9/27.
 */
public class SortStep1Reducer extends Reducer<Text,LongWritable,PageCount,Void> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        String curItem = key.toString();
        Long sum = 0L;
        for (LongWritable longWritable : values) {
            sum += longWritable.get();
        }
        context.write(new PageCount(curItem,sum), null);

    }

}
