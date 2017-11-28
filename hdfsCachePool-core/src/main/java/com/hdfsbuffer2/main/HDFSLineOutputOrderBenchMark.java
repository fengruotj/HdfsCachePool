package com.hdfsbuffer2.main;

import com.hdfsbuffer2.bufferinterface.LinedataOutputHandler;
import com.hdfsbuffer2.model.HdfsLineCachePool;
import com.hdfsbuffer2.task.DataInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

/**
 * locate com.hdfsbuffer2.main
 * Created by 79875 on 2017/4/21.
 * 对HdfsLineCachePool 进行基准测试 缓冲时直接将数据转换成每一行数据 并且装到List链表中 效率低下（适合数据量较小的数据）
 * java -Xmx4028m -Xms4028m -cp hdfsCachePool-core-1.0-SNAPSHOT.jar com.hdfsbuffer2.main.HDFSLineOutputOrderBenchMark /user/root/flinkwordcount/input/resultTweets.txt 10 128
 */
public class HDFSLineOutputOrderBenchMark {
    private static final Log LOG = LogFactory.getLog(HDFSLineOutputOrderBenchMark.class);
    private static List<InputSplit> splits;//输入文件分片的数据类型 InputSplit

    public static void main(String[] args) throws IOException, InterruptedException {
        long startTimeSystemTime= System.currentTimeMillis();
        DataInputFormat dataInputFormat=new DataInputFormat();
        dataInputFormat.setBlockSize(Long.valueOf(args[2])*1024*1024);//设置每个 byteBuffer 的缓存大小
        splits= dataInputFormat.getSplits(args[0]);
        int CachePoolBufferNum=Integer.valueOf(args[1]);//缓冲池缓存Block大小
        HdfsLineCachePool hdfsLineCachePool= HdfsLineCachePool.getInstance(CachePoolBufferNum,splits);
        hdfsLineCachePool.runHDFSCachePool();

        hdfsLineCachePool.hdfsDataOutputOrder(new LinedataOutputHandler() {

            @Override
            public void LinedataOutput(List<Text> stringList ,int hdfsbufferIndex) {

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
