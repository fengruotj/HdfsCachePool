package com.hdfsbuffer2.main;

import com.hdfsbuffer2.bufferinterface.BufferdataOutputHandler;
import com.hdfsbuffer2.model.HdfsCachePool;
import com.hdfsbuffer2.task.DataInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;

/**
 * locate com.hdfsbuffer2.main
 * Created by 79875 on 2017/4/20.
 * 对HdfsCachePool 进行基准测试 直接输出ByteBuffer
 * java -Xmx4028m -Xms4028m -cp hdfsCachePool-core-1.0-SNAPSHOT.jar com.hdfsbuffer2.main.HDFSByteSBufferOutputBenchMark /user/root/flinkwordcount/input/resultTweets.txt 10 128
 * 实验结果：
 * 2017-04-28 16:06:45  [ main:5677 ] - [ INFO ]  ----------------dataOuput over--------------
 * 2017-04-28 16:06:45  [ main:5677 ] - [ INFO ]  startTime:2017-04-28 16:06:39.882
 * 2017-04-28 16:06:45  [ main:5677 ] - [ INFO ]  endTime:2017-04-28 16:06:45.919
 * 2017-04-28 16:06:45  [ main:5677 ] - [ INFO ]  totalTime:34 s------or------0 min
 */
public class HDFSByteSBufferOutputBenchMark {
    private static final Log LOG = LogFactory.getLog(HDFSByteSBufferOutputBenchMark.class);
    private static List<InputSplit> splits;//输入文件分片的数据类型 InputSplit

    public static void main(String[] args) throws IOException, InterruptedException {
        long startTimeSystemTime= System.currentTimeMillis();
        DataInputFormat dataInputFormat=new DataInputFormat();
        dataInputFormat.setBlockSize(Long.valueOf(args[2])*1024*1024);//设置每个 byteBuffer 的缓存大小
        splits= dataInputFormat.getSplits(args[0]);
        int CachePoolBufferNum=Integer.valueOf(args[1]);//缓冲池缓存Block大小
        HdfsCachePool hdfsCachePool= HdfsCachePool.getInstance(CachePoolBufferNum,splits);
        hdfsCachePool.runHDFSCachePool();

        hdfsCachePool.hdfsDataOutputOrder(new BufferdataOutputHandler() {
            @Override
            public void BufferdataOutput(ByteBuffer byteBuffer,int hdfbufferIndex) {

            }
        });
        long endTimeSystemTime = System.currentTimeMillis();
        LOG.info("startTime:"+new Timestamp(startTimeSystemTime));
        LOG.info("endTime:"+new Timestamp(endTimeSystemTime));
        long timelong = (endTimeSystemTime-startTimeSystemTime) / 1000;
        LOG.info("totalTime:"+timelong+" s"+"------or------"+timelong/60+" min");
        hdfsCachePool.releaseHdfsCachePool();//释放fileSystem
        System.exit(0);
    }
}
