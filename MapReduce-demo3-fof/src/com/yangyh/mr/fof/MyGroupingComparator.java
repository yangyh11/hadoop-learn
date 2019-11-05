package com.yangyh.mr.fof;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @description:
 * @author: yangyh
 * @create: 2019-11-05 14:44
 */
public class MyGroupingComparator extends WritableComparator {
    public MyGroupingComparator() {
        super(MyKey.class, true);
    }

    /** 返回结果为0的key为一组**/
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MyKey aKey = (MyKey) a;
        MyKey bKey = (MyKey) b;
        // 按照name判断是否是一组数据
        int result = aKey.getName().compareTo(bKey.getName());
        return result;
    }
}
