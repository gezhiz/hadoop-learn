package com.worthto;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author gezz
 * @description todo
 * @date 2019/9/12.
 */
public class AverageCount {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();//将输入的纯文本文件的数据转化成String
            System.out.println(line);//为了便于程序的调试,输出读入的内容
            //将输入的数据先按行进行分割
            StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
            //分别对每一行进行处理
            while (tokenizerArticle.hasMoreTokens()) {
                //每行按空格划分
                StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
                String strName = tokenizerLine.nextToken();//学生姓名部分
                String strScore = tokenizerLine.nextToken();//成绩部分
                Text name = new Text(strName);//学生姓名
                int scoreInt = Integer.parseInt(strScore);//学生成绩score of student
                context.write(name, new IntWritable(scoreInt));//输出姓名和成绩
            }
        }
    }
}
