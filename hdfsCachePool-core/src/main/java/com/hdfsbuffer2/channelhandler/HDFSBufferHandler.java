package com.hdfsbuffer2.channelhandler;

import com.hdfsbuffer2.bufferinterface.BufferdataOutputHandler;
import com.hdfsbuffer2.model.HdfsCachePool;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * locate com.basic.hdfsbuffer2.handler
 * Created by 79875 on 2017/4/18.
 */
public class HDFSBufferHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSBufferHandler.class);
    private HdfsCachePool hdfsCachePool;

    private int blockPosition=0;
    public HdfsCachePool getHdfsCachePool() {
        return hdfsCachePool;
    }

    public void setHdfsCachePool(HdfsCachePool hdfsCachePool) {
        this.hdfsCachePool = hdfsCachePool;
    }

    public HDFSBufferHandler(HdfsCachePool hdfsCachePool) {
        this.hdfsCachePool = hdfsCachePool;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("HDFSBufferHandler.channelActive");
       new Thread(new HDFSDataOutputChannelTask(ctx)).start();
//        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER) //flush掉所有写回的数据
//                .addListener(ChannelFutureListener.CLOSE); //当flush完成后关闭channel
//        hdfsCachePool.getChannelFuture().channel().close();
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // 当出现异常就关闭连接
        cause.printStackTrace();
        ctx.close();
    }

    public class HDFSDataOutputChannelTask implements Runnable{
        private ChannelHandlerContext ctx;

        public HDFSDataOutputChannelTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {

            hdfsCachePool.hdfsDataOutputOrder(new BufferdataOutputHandler() {
                @Override
                public void BufferdataOutput(ByteBuffer byteBuffer,int bufferindex) {
                    while (byteBuffer.hasRemaining()){
                        int length=Math.min(4096,byteBuffer.remaining());
                        byte[] buf=new byte[length];
                        byteBuffer.get(buf,0,length);
                        ByteBuf byteBuf=ctx.alloc().buffer(length);
                        byteBuf.writeBytes(buf);
                        ctx.writeAndFlush(byteBuf);
                        //byteBuf.release();
                    }
                }
            });

        }
    }

}
