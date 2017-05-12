package com.hdfsbuffer.spout;

import com.hdfsbuffer2.BufferLineReader;
import com.hdfsbuffer2.model.HdfsCachePool;
import com.hdfsbuffer2.task.DataInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * locate com.hdfsbuffer.spout
 * Created by 79875 on 2017/4/17.
 */
public class HDFSBufferSpout extends BaseRichSpout {
    private Fields outputFields;

    private long Totalrows=0L;
    private long blockrows=0L;
    private HdfsCachePool hdfsCachePool;

    private int blockPosition=0;

    private SpoutOutputCollector outputCollector;

    private int bufferNum;
    private String inputFile;

    private Long bufferSize=null;
    public HDFSBufferSpout() {

    }

    public Long getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(Long bufferSize) {
        this.bufferSize = bufferSize;
    }

    public HDFSBufferSpout(int bufferNum, String inputFile) throws IOException, InterruptedException {
        this.bufferNum=bufferNum;
        this.inputFile=inputFile;
    }

    public HDFSBufferSpout withOutputFields(String... fields) {
        this.outputFields = new Fields(fields);
        return this;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            DataInputFormat dataInputFormat=new DataInputFormat();
            if(bufferSize!=null)
                dataInputFormat.setBlockSize(bufferSize);
            List<InputSplit> splits = dataInputFormat.getSplits(inputFile);
            this.hdfsCachePool=HdfsCachePool.getInstance(10,splits);
        } catch (Exception e) {
            e.printStackTrace();
        }
        hdfsCachePool.runHDFSCachePool();
        this.outputCollector=spoutOutputCollector;
    }

    @Override
    public void nextTuple(){
        try {
            datoutputTuple();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     *  输出数据Tuple
     * @throws IOException
     * @throws InterruptedException
     */
    public void datoutputTuple() throws IOException, InterruptedException {
        BufferLineReader bufferLineReader=new BufferLineReader(hdfsCachePool);
        Text text=new Text();
        try {
            while (bufferLineReader.readLine(text)!=0){
                //System.out.println(text.toString());
                outputCollector.emit(new Values(text.toString()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(this.outputFields);
    }
}
