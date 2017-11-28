package com.hdfsbuffer2.main;

import com.hdfsbuffer2.ByteBufferLineReader;
import com.hdfsbuffer2.bufferinterface.BufferdataOutputHandler;
import com.hdfsbuffer2.model.HdfsCachePool;
import com.hdfsbuffer2.task.DataInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;

/**
 * locate com.hdfsbuffer2.main
 * Created by 79875 on 2017/4/28.
 * HDFSCachePool 每块ByteBuffer缓冲区 用ByteBufferLineReader 转换成每一行 进行输出
 * java -Xmx4028m -Xms4028m -cp hdfsCachePool-core-1.0-SNAPSHOT.jar com.hdfsbuffer2.main.HDFSByteBufferLineReaderOutputBenchMark /user/root/flinkwordcount/input/resultTweets.txt 10 128
 * 2017-04-30 12:54:01  [ main:49052 ] - [ INFO ]  startTime:2017-04-30 12:53:12.183
 * 2017-04-30 12:54:01  [ main:49052 ] - [ INFO ]  endTime:2017-04-30 12:54:01.605
 * 2017-04-30 12:54:01  [ main:49052 ] - [ INFO ]  totalTime:49 s------or------0 min
 */
public class HDFSByteBufferLineReaderOutputBenchMark {
    private static final Log LOG = LogFactory.getLog(HDFSBufferLineReaderOutputBenchMark.class);
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
            public void BufferdataOutput(ByteBuffer byteBuffer, int bufferindex) {
                ByteBufferLineReader byteBufferLineReader=new ByteBufferLineReader(byteBuffer);
                Text text=new Text();
                try {
                    while (byteBufferLineReader.readLine(text)!=0){
//                        if(byteBuffer.remaining()<=763810 && bufferindex==26)
//                            LOG.info(text.toString());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        long endTimeSystemTime = System.currentTimeMillis();
        LOG.info("startTime:"+new Timestamp(startTimeSystemTime));
        LOG.info("endTime:"+new Timestamp(endTimeSystemTime));
        long timelong = (endTimeSystemTime-startTimeSystemTime) / 1000;
        LOG.info("totalTime:"+timelong+" s"+"------or------"+timelong/60+" min");
        System.exit(0);
    }
}
