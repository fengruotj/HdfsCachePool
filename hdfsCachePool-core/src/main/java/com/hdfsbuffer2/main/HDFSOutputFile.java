package com.hdfsbuffer2.main;

import com.hdfsbuffer2.bufferinterface.BufferdataOutputHandler;
import com.hdfsbuffer2.model.HdfsCachePool;
import com.hdfsbuffer2.task.DataInputFormat;
import com.hdfsbuffer2.util.HdfsOperationUtil;
import com.hdfsbuffer2.util.PropertiesUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;

/**
 * locate com.hdfsbuffer2.main
 * Created by mastertj on 2017/11/14.
 *  java -Xmx4028m -Xms4028m -cp hdfsCachePool-core-1.0-SNAPSHOT.jar com.hdfsbuffer2.main.HDFSOutputFile /user/root/flinkwordcount/input/resultTweets.txt /root/TJ/a.txt 10 128
 */
public class HDFSOutputFile {
    private static final Log LOG = LogFactory.getLog(HdfsCachePool.class);
    private static List<InputSplit> splits;//输入文件分片的数据类型 InputSplit
    private static FileOutputStream fileOutputStream;
    public static void main(String[] args) throws IOException, InterruptedException {
        long startTimeSystemTime= System.currentTimeMillis();
        PropertiesUtil.init("/hdfscachepool.properties");
        fileOutputStream=new FileOutputStream(new File(args[1]),true);

        DataInputFormat dataInputFormat=new DataInputFormat();
        dataInputFormat.setBlockSize(Long.valueOf(args[3])*1024*1024);//设置每个 byteBuffer 的缓存大小
        splits= dataInputFormat.getSplits(args[0]);
        int CachePoolBufferNum=Integer.valueOf(args[2]);//缓冲池缓存Block大小
        HdfsCachePool hdfsCachePool= HdfsCachePool.getInstance(CachePoolBufferNum,splits);
        hdfsCachePool.runHDFSCachePool();

        hdfsCachePool.hdfsDataOutputOrder(new BufferdataOutputHandler() {
            @Override
            public void BufferdataOutput(ByteBuffer byteBuffer, int hdfbufferIndex) {
                try {
                    fileOutputStream.write(byteBuffer.array(),0,byteBuffer.limit());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        long endTimeSystemTime = System.currentTimeMillis();
        LOG.info("startTime:"+new Timestamp(startTimeSystemTime));
        LOG.info("endTime:"+new Timestamp(endTimeSystemTime));
        long timelong = (endTimeSystemTime-startTimeSystemTime) / 1000;
        LOG.info("totalTime:"+timelong+" s"+"------or------"+timelong/60+" min");
        fileOutputStream.close();
        HdfsOperationUtil.releaseHDFSConnections();
        System.exit(0);
    }
}
