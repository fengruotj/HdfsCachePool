package com.hdfsbuffer2;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by 79875 on 2017/4/7.
 */
public class BufferLineReaderTemp {
    private int bufferPosn;
    private int bufferLength;
    private static final byte CR = 13;
    private static final byte LF = 10;
    private ByteBuffer byteBuffer;

    public BufferLineReaderTemp(ByteBuffer byteBuffer) {
        this.bufferPosn = 0;
        this.byteBuffer=byteBuffer;
        this.bufferLength=byteBuffer.limit();
    }

    public int readLine(Text text, int maxLineLength, int maxBytesToConsume) throws IOException {
       return this.readDefaultLine(text, maxLineLength, maxBytesToConsume);
    }

    private int readDefaultLine(Text text, int maxLineLength, int maxBytesToConsume) throws IOException {
        text.clear();
        int txtLength = 0;
        long bytesConsumed = 0L;
        int txtPos=bufferPosn;
        if(!byteBuffer.hasRemaining()){
            return 0;
        }
        while (bufferPosn<byteBuffer.limit()){
            if(byteBuffer.array()[bufferPosn]==CR){
                // /r
                bufferPosn++;
                if(byteBuffer.array()[bufferPosn]==LF){
                    // /r /n
                    bufferPosn++;
                    break;
                }
                break;
            }
            if(byteBuffer.array()[bufferPosn]==LF){
                // /n
                bufferPosn++;
                break;
            }
            bufferPosn++;
            txtLength++;
        }
        byte[] buf=new byte[bufferPosn-txtPos];
        byteBuffer.get(buf,0,bufferPosn-txtPos);
        text.append(buf,0,txtLength);
        bytesConsumed=bufferPosn-txtPos;
//        do {
//            int startPosn = this.bufferPosn;
//            if(this.bufferPosn >= this.bufferLength) {
//                startPosn = this.bufferPosn = 0;
//                if(prevCharCR) {
//                    ++bytesConsumed;
//                }
//
//                this.bufferLength = this.fillBuffer(this.byteBuffer, this.buffer, prevCharCR);
//                if(this.bufferLength <= 0) {
//                    break;
//                }
//            }
//
//            while(this.bufferPosn < this.bufferLength) {
//                if(this.buffer[this.bufferPosn] == 10) {
//                    newlineLength = prevCharCR?2:1;
//                    ++this.bufferPosn;
//                    break;
//                }
//
//                if(prevCharCR) {
//                    newlineLength = 1;
//                    break;
//                }
//
//                prevCharCR = this.buffer[this.bufferPosn] == 13;
//                ++this.bufferPosn;
//            }
//
//             int readLength = this.bufferPosn - startPosn;
//            if(prevCharCR && newlineLength == 0) {
//                --readLength;
//            }
//
//            bytesConsumed += (long)readLength;
//            int appendLength = readLength - newlineLength;
//            if(appendLength > maxLineLength - txtLength) {
//                appendLength = maxLineLength - txtLength;
//            }
//
//            if(appendLength > 0) {
//                str.append(this.buffer, startPosn, appendLength);
//                txtLength += appendLength;
//            }
//        } while(newlineLength == 0 && bytesConsumed < (long)maxBytesToConsume);

        if(bytesConsumed > 2147483647L) {
            throw new IOException("Too many bytes before newline: " + bytesConsumed);
        } else {
            return (int)bytesConsumed;
        }
    }



    public int readLine(Text str, int maxLineLength) throws IOException {
        return this.readLine(str, maxLineLength, 2147483647);
    }

    public int readLine(Text str) throws IOException {
        return this.readLine(str, 2147483647, 2147483647);
    }
}
