package com.hdfsbuffer2.main;

import com.hdfsbuffer2.BufferLineReader;
import com.hdfsbuffer2.model.HdfsCachePool;
import com.hdfsbuffer2.task.DataInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

/**
 * locate com.hdfsbuffer2.main
 * Created by 79875 on 2017/4/28.
 * HDFSCachePoolBuffer 一次性读取每一行数据基准测试
 * java -Xmx4028m -Xms4028m -cp hdfsCachePool-core-1.0-SNAPSHOT.jar com.hdfsbuffer2.main.HDFSBufferLineReaderOutputBenchMark /user/root/flinkwordcount/input/resultTweets.txt 10 128
 * 2017-04-28 16:09:16  [ main:13647 ] - [ INFO ]  startTime:2017-04-28 16:09:02.328
 * 2017-04-28 16:09:16  [ main:13647 ] - [ INFO ]  endTime:2017-04-28 16:09:16.315
 * 2017-04-28 16:09:16  [ main:13647 ] - [ INFO ]  totalTime:48 s------or------0 mi
 */
public class HDFSBufferLineReaderOutputBenchMark {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSBufferLineReaderOutputBenchMark.class);
    private static List<InputSplit> splits;//输入文件分片的数据类型 InputSplit

    public static void main(String[] args) throws IOException, InterruptedException {
        long startTimeSystemTime= System.currentTimeMillis();
        DataInputFormat dataInputFormat=new DataInputFormat();
        dataInputFormat.setBlockSize(Long.valueOf(args[2])*1024*1024);//设置每个 byteBuffer 的缓存大小
        splits= dataInputFormat.getSplits(args[0]);
        int CachePoolBufferNum=Integer.valueOf(args[1]);//缓冲池缓存Block大小
        HdfsCachePool hdfsCachePool= HdfsCachePool.getInstance(CachePoolBufferNum,splits);
        hdfsCachePool.runHDFSCachePool();

        BufferLineReader bufferLineReader=new BufferLineReader(hdfsCachePool);
        Text text=new Text();
        try {
            while (bufferLineReader.readLine(text)!=0){
                //System.out.println(text.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTimeSystemTime = System.currentTimeMillis();
        LOG.info("startTime:"+new Timestamp(startTimeSystemTime));
        LOG.info("endTime:"+new Timestamp(endTimeSystemTime));
        long timelong = (endTimeSystemTime-startTimeSystemTime) / 1000;
        LOG.info("totalTime:"+timelong+" s"+"------or------"+timelong/60+" min");
        System.exit(0);
    }
}
