package com.hdfsbuffer2.model;

import com.hdfsbuffer2.bufferinterface.BufferdataOutputHandler;
import com.hdfsbuffer2.channelhandler.HDFSBufferHandler;
import com.hdfsbuffer2.task.DataInputTask;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by 79875 on 2017/4/1.'
 */
public class HdfsCachePool {
    private static final Log LOG = LogFactory.getLog(HdfsCachePool.class);
    private static HdfsCachePool instance;//缓存池唯一实例

    private HDFSBuffer[] bufferArray;

    private int bufferNum = 10;

    private int positionBlock = 0;    //当前读取HDFS文件Block下标

    private List<InputSplit> inputSplitList = new ArrayList<>();

    private ChannelFuture channelFuture;//ServerSocket ChannelFuture
    //    private boolean isbufferfinish=false;   //是否缓存数据完毕

    public ChannelFuture getChannelFuture() {
        return channelFuture;
    }

    public void setChannelFuture(ChannelFuture channelFuture) {
        this.channelFuture = channelFuture;
    }

    /**
     * 线程池
     * 创建一个可缓存线程池，如果线程池长度超过处理需要，可灵活回收空闲线程，若无可回收，则新建线程。
     */
    ExecutorService executor = Executors.newCachedThreadPool();
//    ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 20, 200, TimeUnit.MILLISECONDS,
//            new LinkedBlockingQueue<Runnable>());


    public HdfsCachePool(int bufferNum, List<InputSplit> inputSplitList) throws IOException, InterruptedException {
        this.bufferNum = bufferNum;
        this.inputSplitList = inputSplitList;
        bufferArray = new HDFSBuffer[bufferNum];
        for(int i=0;i<bufferNum;i++){
            bufferArray[i]=new HDFSBuffer(i);
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
            bufferArray[i]=new HDFSBuffer(i);
        }
        for (int i = 0; i < bufferNum; i++) {
            bufferArray[i].byteBuffer = ByteBuffer.allocate((int) inputSplitList.get(i).getLength());//创建一个128M大小的字节缓存区
        }
    }

    public void setInstance(int bufferindex, InputSplit inputSplit) throws IOException, InterruptedException {
        bufferArray[bufferindex].setBufferFinished(false);
        bufferArray[bufferindex].byteBuffer = ByteBuffer.allocate((int) inputSplit.getLength());
    }


    /**
     * 得到唯一实例
     *
     * @return
     */
    public synchronized static HdfsCachePool getInstance() {
//        if(instance == null){
//            instance = new HdfsCachePool();
//        }
        return instance;
    }

    public synchronized static HdfsCachePool getInstance(int bufferNum, List<InputSplit> inputSplitList) throws IOException, InterruptedException {
        instance = new HdfsCachePool(bufferNum, inputSplitList);
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
        DataInputTask dataInputTask = new DataInputTask(this.bufferArray[bufferBlock], inputSplit,blockNum);
        executor.execute(dataInputTask);
    }

    public HDFSBuffer[] getBufferArray() {
        return bufferArray;
    }


    /**
     * clear缓冲区数组
     * position = 0;
     * limit = capacity;
     * mark = -1;
     */
    public void clearBufferArray() {
        for (int i = 0; i < bufferArray.length; i++) {
            bufferArray[i].byteBuffer.clear();
        }
    }



    /**
     * 缓冲区输出完毕继续缓存下一块Block
     * @param Num 当前输出缓冲块的下标
     */
    public void bufferNextBlock(int Num) throws IOException, InterruptedException {
        int inputSplitNum=bufferArray[Num].getInputSplitNum();
        if(inputSplitNum<inputSplitList.size()){
            LOG.debug("bufferNextBlock------------ NUM: "+Num+" inputSplitNum:"+inputSplitNum);
            bufferArray[Num].setBufferOutFinished(false);
            datainputBuffer(Num,inputSplitList.get(inputSplitNum),inputSplitNum);
            bufferArray[Num].setInputSplitNum(inputSplitNum+bufferNum);
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
        return bufferArray[blocknum].isBufferOutFinished();
    }

    /**
     *  是否buferBlock缓存完毕
     * @param blocknum blocknum下标
     * @return
     */
    public boolean isBufferBlockFinished(int blocknum){
        return bufferArray[blocknum].isBufferFinished();
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
                        //Thread.sleep(100);
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
     * 将缓冲池的数据打开一个 Netty Channle通道通信
     * 使用的是Netty通信框架
     * @param port 打开端口号
     */
    public void dataOutputChannel(int port){
        //EventLoopGroup是用来处理IO操作的多线程事件循环器
        //bossGroup 用来接收进来的连接
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        //workerGroup 用来处理已经被接收的连接
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            //启动 NIO 服务的辅助启动类
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    //配置 Channel
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            // 注册handler
                            ch.pipeline().addLast(new HDFSBufferHandler(getInstance()));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // 绑定端口，开始接收进来的连接
            channelFuture = b.bind(port).sync();
            // 等待服务器 socket 关闭 。
            channelFuture.channel().closeFuture().sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    /**
     * HdfsDataOutPut的辅助函数 得到当前hdfsDataOutput可以读取的blockNum
     * @param blockPosition
     * @return
     */
    public int getActiveBufferNum(int blockPosition){
        int bufferNum=getBufferArray().length;
        List<InputSplit> inputSplitList=getInputSplitList();
        return  (inputSplitList.size()-blockPosition) < bufferNum ?inputSplitList.size() % bufferNum : bufferNum;
    }

    /**
     * HdfsBufferDataOutput 保持数据的有序性
     * bufferdataOutputHandler 用来处理顺序的读取的ByteBuffer
     * @param bufferdataOutputHandler
     */
    public void hdfsDataOutputOrder(BufferdataOutputHandler bufferdataOutputHandler){
        int blockPosition=0;
        while (true){
            //if(hdfsCachePool.isIsbufferfinish()){
            //可以开始读取HdfsCachePool
            int activeBufferNum = getActiveBufferNum(blockPosition);
            for(int i=0;i<activeBufferNum;i++){
                while (!isBufferBlockFinished(i)){
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                getBufferArray()[i].setBufferOutFinished(false);
                ByteBuffer byteBuffer = getBufferArray()[i].byteBuffer;
                LOG.debug("---------start--------"+ byteBuffer +" num:"+i+" blockPosition: "+blockPosition);

                bufferdataOutputHandler.BufferdataOutput(byteBuffer,i);

                getBufferArray()[i].setBufferOutFinished(true);
                getBufferArray()[i].setBufferFinished(false);
                blockPosition++;
            }
            if(blockPosition>=getInputSplitList().size()){
                LOG.info("----------------dataOuput over--------------");
                break;
            }
        }
    }

    public int getBufferNum() {
        return bufferNum;
    }

    public void setBufferNum(int bufferNum) {
        this.bufferNum = bufferNum;
    }
}
