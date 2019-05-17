package com.hdfsbuffer.main;

import com.hdfsbuffer.output.DataOutputKafka;
import com.hdfsbuffer.util.KafkaUtil;
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
 * locate com.hdfsbuffer.main
 * Created by 79875 on 2017/4/22.
 * java -Xmx4028m -Xms4028m -cp hdfsCachePool-kafka-1.0-SNAPSHOT.jar com.hdfsbuffer.main.KafkaLineDataOutputMain /user/root/wordcount/input/data_658038657_40/data_658038657_40.log 10 testTopic 9
 * 实验结果 ：testTopic	9	3	100	0	1	0		184266.69	3849890561  data_658038657_40.log
 */
public class KafkaLineDataOutputMain {
    private static final Log LOG = LogFactory.getLog(KafkaDataOutputMain.class);
    private static List<InputSplit> splits;//输入文件分片的数据类型 InputSplit
    private static DataInputFormat dataInputFormat=new DataInputFormat();
    private static DataOutputKafka dataOutputKafka;
    private static KafkaUtil kafkaUtil=new KafkaUtil();
    private static long Totalrows=0;
    public static void main(String[] args) throws IOException, InterruptedException {
        long startTimeSystemTime= System.currentTimeMillis();
        dataInputFormat.setBlockSize(Long.valueOf(1024*1024*128));
        splits= dataInputFormat.getSplits(args[0]);
        int CachePoolBufferNum=Integer.valueOf(args[1]);//缓冲池缓存Block大小
        final String kafkaTopic=args[2];//kafka的Topci
        final int kafkaPartitionsNum=Integer.valueOf(args[3]); //kafkaTopic 的分区个数保证数据均匀分布在Kafka分区中
        HdfsLineCachePool hdfsCachePool= HdfsLineCachePool.getInstance(CachePoolBufferNum,splits);
        hdfsCachePool.runHDFSCachePool();

        hdfsCachePool.hdfsDataOutputOrder(new LinedataOutputHandler() {
            public void LinedataOutput(List<Text> stringList,int hdfsbufferIndex) {
                for(int i=0; i< stringList.size();i++){
                    kafkaUtil.publishOrderMessage(kafkaTopic, kafkaPartitionsNum, (int) Totalrows,stringList.get(i).toString());
                    Totalrows++;
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
