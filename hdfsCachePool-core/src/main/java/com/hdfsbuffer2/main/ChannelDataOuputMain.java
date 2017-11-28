package com.hdfsbuffer2.main;

import com.hdfsbuffer2.model.HdfsCachePool;
import com.hdfsbuffer2.task.DataInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

/**
 * locate com.hdfsbuffer2
 * Created by 79875 on 2017/4/18.
 * java -Xmx4028m -Xms4028m -cp hdfsCachePool-core-1.0-SNAPSHOT.jar com.hdfsbuffer2.main.ChannelDataOuputMain /user/root/flinkwordcount/input/resultTweets.txt 10
 */
public class ChannelDataOuputMain {
    private static final Log LOG = LogFactory.getLog(ChannelDataOuputMain.class);
    private static List<InputSplit> splits;//输入文件分片的数据类型 InputSplit
    private static DataInputFormat dataInputFormat=new DataInputFormat();

    public static void main(String[] args) throws IOException, InterruptedException {
        long startTimeSystemTime= System.currentTimeMillis();
        splits= dataInputFormat.getSplits(args[0]);
        int CachePoolBufferNum=Integer.valueOf(args[1]);//缓冲池缓存Block大小
        HdfsCachePool hdfsCachePool= HdfsCachePool.getInstance(CachePoolBufferNum,splits);
        hdfsCachePool.runHDFSCachePool();

        hdfsCachePool.dataOutputChannel(9999);
        long endTimeSystemTime = System.currentTimeMillis();
        LOG.info("startTime:"+new Timestamp(startTimeSystemTime));
        LOG.info("endTime:"+new Timestamp(endTimeSystemTime));
        long timelong = (endTimeSystemTime-startTimeSystemTime) / 1000;
        LOG.info("totalTime:"+timelong+" s"+"------or------"+timelong/60+" min");
        System.exit(0);
    }
}
