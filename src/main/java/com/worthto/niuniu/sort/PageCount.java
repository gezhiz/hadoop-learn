package com.worthto.niuniu.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author gezz
 * @description todo
 * @date 2019/9/27.
 */
public class PageCount implements WritableComparable<PageCount> {
    private String page;
    private Long count;

    public PageCount() {}

    public PageCount(String page, Long count) {
        this.page = page;
        this.count = count;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public int compareTo(PageCount o) {
        return count - o.getCount() == 0 ? o.getPage().compareTo(this.page) : (int) (o.getCount() - this.count);
    }

    @Override
    public String toString() {
        return page + "\t" + count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(page);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.page = in.readUTF();
        this.count = in.readLong();
    }
}
