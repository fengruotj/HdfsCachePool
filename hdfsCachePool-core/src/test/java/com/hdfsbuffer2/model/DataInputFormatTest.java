package com.hdfsbuffer2.model;

import com.hdfsbuffer2.task.DataInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * locate com.hdfsbuffer2.model
 * Created by 79875 on 2017/6/2.
 */
public class DataInputFormatTest {
    private List<InputSplit> splits;//输入文件分片的数据类型 InputSplit
    private DataInputFormat dataInputFormat=new DataInputFormat();

    @Test
    public void testDataInput() throws IOException {
        List<InputSplit> splits = dataInputFormat.getSplits("/user/root/hive_remote/warehouse/psn1");
        System.out.println(splits.size());
    }
}
