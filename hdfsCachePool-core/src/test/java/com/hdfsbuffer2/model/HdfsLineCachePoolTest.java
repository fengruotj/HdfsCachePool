package com.hdfsbuffer2.model;

import com.hdfsbuffer2.task.DataInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * locate com.hdfsbuffer2.model
 * Created by 79875 on 2017/4/21.
 */
public class HdfsLineCachePoolTest {
    @Test
    public void runHDFSCachePool() throws Exception {
        DataInputFormat dataInputFormat=new DataInputFormat();
        dataInputFormat.setBlockSize(Long.valueOf(128*1024*1024));//设置每个 byteBuffer 的缓存大小
        List<InputSplit> splits= dataInputFormat.getSplits("/user/root/flinkwordcount/input/resultTweets.txt");
        int CachePoolBufferNum=Integer.valueOf(10);//缓冲池缓存Block大小
        HdfsLineCachePool hdfsLineCachePool= HdfsLineCachePool.getInstance(CachePoolBufferNum,splits);
        hdfsLineCachePool.runHDFSCachePool();
        while (true){
            Thread.sleep(100);
        }
    }

    @Test
    public void ListTest(){
        List<Text> textList=new ArrayList<>();
        Text text=new Text();
        text.set("i am first");
        textList.add(text);
        text.set("i am second");
        textList.add(text);
        System.out.println(textList);
    }

    @Test
    public void ListStringTest(){
        List<String> textList=new ArrayList<>();
        Text text=new Text();
        text.set("i am first");
        textList.add(text.toString());
        text.set("i am second");
        textList.add(text.toString());
        System.out.println(textList);
    }

//    @Test
//    public void MemoryListStringTest(){
//        List<String> textList=new ArrayList<>();
//        String str="tanjie";
//        System.out.println(SizeOfObject.sizeOf(str));
//        textList.add("tanjie");
//        System.out.println(SizeOfObject.sizeOf(textList));
//    }
}
