package com.hdfsbuffer2.model;

import com.hdfsbuffer2.bufferinterface.LinedataOutputHandler;
import com.hdfsbuffer2.task.LineDataInputTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * locate com.hdfsbuffer2.model
 * Created by 79875 on 2017/4/21.
 */
public class HdfsLineCachePool {

    private static final Log LOG = LogFactory.getLog(HdfsLineCachePool.class);
    private static HdfsLineCachePool instance;//缓存池唯一实例

    private HDFSLineBuffer[] LinebufferArray;

    private int bufferNum = 10;

    private int positionBlock = 0;    //当前读取HDFS文件Block下标

    private List<InputSplit> inputSplitList = new ArrayList<>();

    /**
     * 线程池
     */
    ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 20, 200, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());


    public HdfsLineCachePool(int bufferNum, List<InputSplit> inputSplitList) throws IOException, InterruptedException {
        this.bufferNum = bufferNum;
        this.inputSplitList = inputSplitList;
        LinebufferArray = new HDFSLineBuffer[bufferNum];
        for(int i=0;i<bufferNum;i++){
            LinebufferArray[i]=new HDFSLineBuffer(i);
        }
    }

    /**
     * 初始化HDFSCachePool 缓冲池
     *
     * @param inputSplitList
     * @throws IOException
     * @throws InterruptedException
     */
    public void init(List<InputSplit> inputSplitList) throws IOException, InterruptedException {
//        if(bufferArray!=null)//如果缓冲数组不为空则首先清除缓冲区
//            clearBufferArray();
        for(int i=0;i<bufferNum;i++){
            LinebufferArray[i]=new HDFSLineBuffer(i);
        }
        for (int i = 0; i < bufferNum; i++) {
            LinebufferArray[i].lineList = new ArrayList<>();
        }
    }

    public void setInstance(int bufferindex, InputSplit inputSplit) throws IOException, InterruptedException {
        LinebufferArray[bufferindex].setBufferFinished(false);
        LinebufferArray[bufferindex].lineList.clear();
        System.gc();
        //LinebufferArray[bufferindex].lineList =new ArrayList<>();
    }

    /**
     * 得到唯一实例
     *
     * @return
     */
    public synchronized static HdfsLineCachePool getInstance() {
//        if(instance == null){
//            instance = new HdfsCachePool();
//        }
        return instance;
    }

    public synchronized static HdfsLineCachePool getInstance(int bufferNum, List<InputSplit> inputSplitList) throws IOException, InterruptedException {
        instance = new HdfsLineCachePool(bufferNum, inputSplitList);
        return instance;
    }

    /**
     * 缓存bufferBlock缓冲池数据块
     *
     * @param bufferBlock bufferBlock块编号
     * @param inputSplit  hdfs文件分片
     * @throws IOException
     */
    public void datainputBuffer(int bufferBlock, InputSplit inputSplit, int blockNum) throws IOException, InterruptedException {
        setInstance(bufferBlock,inputSplit);
        LineDataInputTask dataInputTask = new LineDataInputTask(this.LinebufferArray[bufferBlock], inputSplit,blockNum);
        executor.execute(dataInputTask);
    }

    public HDFSLineBuffer[] getLinebufferArray() {
        return LinebufferArray;
    }

    public void setLinebufferArray(HDFSLineBuffer[] linebufferArray) {
        LinebufferArray = linebufferArray;
    }


    /**
     * 缓冲区输出完毕继续缓存下一块Block
     * @param Num 当前输出缓冲块的下标
     */
    public void bufferNextBlock(int Num) throws IOException, InterruptedException {
        int inputSplitNum=LinebufferArray[Num].getInputSplitNum();
        if(inputSplitNum<inputSplitList.size()){
            LOG.debug("bufferNextBlock------------ NUM: "+Num+" inputSplitNum:"+inputSplitNum);
            LinebufferArray[Num].setBufferOutFinished(false);
            datainputBuffer(Num,inputSplitList.get(inputSplitNum),inputSplitNum);
            LinebufferArray[Num].setInputSplitNum(inputSplitNum+bufferNum);
            positionBlock++;
        }
    }

    /**
     * 是否buferBlock缓存输出完毕
     * @param blocknum blocknum下标
     * @return
     */
    public boolean isBufferBlockoutFinished(int blocknum){
//        ByteBuffer byteBuffer = bufferArray[blocknum].byteBuffer;
//        if(byteBuffer.hasRemaining())
//            return false;
//        else return true;
        return LinebufferArray[blocknum].isBufferOutFinished();
    }

    /**
     *  是否buferBlock缓存完毕
     * @param blocknum blocknum下标
     * @return
     */
    public boolean isBufferBlockFinished(int blocknum){
        return LinebufferArray[blocknum].isBufferFinished();
    }

    public List<InputSplit> getInputSplitList() {
        return inputSplitList;
    }

    public void setInputSplitList(List<InputSplit> inputSplitList) {
        this.inputSplitList = inputSplitList;
    }

    public class HdfsCachePoolControlTask implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    for(int i=0;i<bufferNum;i++){
                        if(isBufferBlockoutFinished(i)){
                            bufferNextBlock(i);
                        }
                    }
                    if(positionBlock>=inputSplitList.size()){
                        //bufferoutputOver=true;
                        executor.shutdown();//顺序关闭，执行以前提交的任务，但不接受新任务
                        if(executor.isTerminated()){
                            //所有子线程都结束任务
                            LOG.info("----------------dataInput over--------------");
                            break;
                        }
                    }
                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 启动HdfsCachePool 缓充池
     */
    public void runHDFSCachePool(){
        HdfsCachePoolControlTask hdfsCachePoolControlTask=new HdfsCachePoolControlTask();
        Thread thread2 = new Thread(hdfsCachePoolControlTask);
        thread2.start();
    }

    /**
     * HdfsDataOutPut的辅助函数 得到当前hdfsDataOutput可以读取的blockNum
     * @param blockPosition
     * @return
     */
    public int getActiveBufferNum(int blockPosition){
        int bufferNum=LinebufferArray.length;
        List<InputSplit> inputSplitList=getInputSplitList();
        return  (inputSplitList.size()-blockPosition)<= bufferNum ?inputSplitList.size() % bufferNum : bufferNum;
    }

    /**
     * HdfsBufferDataOutput 保持数据的有序性
     * bufferdataOutputHandler 用来处理顺序的读取的ByteBuffer
     * @param linedataOutputHandler
     */
    public void hdfsDataOutputOrder(LinedataOutputHandler linedataOutputHandler){
        int blockPosition=0;
        while (true){
            //if(hdfsCachePool.isIsbufferfinish()){
            //可以开始读取HdfsCachePool
            int activeBufferNum = getActiveBufferNum(blockPosition);
            for(int i=0;i<activeBufferNum;i++){
                while (!isBufferBlockFinished(i)){
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                LinebufferArray[i].setBufferOutFinished(false);
                List<Text> lineList = LinebufferArray[i].lineList;
                LOG.debug("---------start-------- "+ lineList.size() +" num:"+i+" blockPosition: "+blockPosition);

                linedataOutputHandler.LinedataOutput(lineList,i);

                System.gc();
                LinebufferArray[i].setBufferOutFinished(true);
                LinebufferArray[i].setBufferFinished(false);
                blockPosition++;
            }
            if(blockPosition>=getInputSplitList().size()){
                LOG.info("----------------dataOuput over--------------");
                break;
            }
        }
    }

}
