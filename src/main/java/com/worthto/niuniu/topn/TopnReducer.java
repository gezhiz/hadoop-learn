package com.worthto.niuniu.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author gezz
 * @description todo
 * @date 2019/9/27.
 */
public class TopnReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

    /**
     * topn参数
     */
    private static int topn = 3;
    /**
     * 记录本次map任务计算出的结果的前几位
     */
    private TreeMap<PageCount,String> cacheMap = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        String curItem = key.toString();
        Long sum = 0L;
        for (LongWritable longWritable : values) {
            sum += longWritable.get();
        }
        cacheMap.put(new PageCount(curItem,sum),curItem);
        if (cacheMap.size() > topn) {
            cacheMap.remove(cacheMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // NOTHING
        for (Map.Entry<PageCount,String> entry : cacheMap.entrySet()) {
            PageCount pageCount = entry.getKey();
            context.write(new Text(pageCount.getPage()),new LongWritable(pageCount.getCount()));
        }

    }
}
