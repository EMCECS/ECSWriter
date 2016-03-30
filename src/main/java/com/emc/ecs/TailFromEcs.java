package com.emc.ecs;

import com.emc.ecs.s3.sample.ECSS3Factory;
import com.emc.object.Range;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.request.GetObjectRequest;
import com.emc.object.s3.request.ListObjectsRequest;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.List;

/**
 * Created by conerj on 3/24/16.
 */
public class TailFromEcs implements Runnable {
    private S3Client s3Client;
    private String bucket;
    private String key;
    private int action;
    long bytesFromEnd = 0;


    protected static final int TAILBYTE = 0;
    protected static final int TAIL = 1;

    public TailFromEcs(S3Client s3Client, String bucket, String key, int action ) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.key = key;
        this.action = action;
    }
    /*
     * not used.
     * It reads byte by byte from
     * This won't tail the whole file at once. Just a range.
     * So it could be called repeatedly for an extremely large file
     */

    public void tailRangeByByte(long offset) throws java.io.IOException {
        int tmpContent;
        //Range range = new Range((long) offset, (long) EcsBufferedWriter.BUFFER_SIZE);
        //Range range = new Range((long) offset, (long) offset + EcsBufferedWriter.BUFFER_SIZE);
        Range range = Range.fromOffsetLength(offset, EcsBufferedWriter.BUFFER_SIZE);
        if (this.getObjectListResult() < offset + EcsBufferedWriter.BUFFER_SIZE) {
            System.err.println("JMC changing up the range");
            range = Range.fromOffset(offset);
        }

        InputStream is = this.s3Client.readObjectStream(this.bucket, key, range);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        while((tmpContent = br.read()) != -1) {
            System.out.print((char) tmpContent);
        }
    }

    public void tailByte() {
        long offset = 0;
        boolean keepTailing = true;
        try {
            if (this.s3Client == null) {
                throw (new Exception("s3Client is null for some reason"));
            }

            while (keepTailing) {
                try {
                    tailRangeByByte(offset);
                    offset += EcsBufferedWriter.BUFFER_SIZE;
                }
                catch(com.emc.object.s3.S3Exception e) {
                    System.err.println("Going to retry after S3Exception");
                }
                catch(java.io.IOException ioe) {
                    //ioe.printStackTrace();
                    System.err.println("Going to retry after IOException");
                }
                catch(Exception e) {
                    //e.printStackTrace();
                    System.err.println("Going to retry after some kind of Exception");
                }
            }
        }
        catch(java.lang.NullPointerException e) {
            e.printStackTrace();
            //System.out.println("The object may not exist");
        }
        //java.net.URISyntaxException
        catch(Exception e) {
            e.printStackTrace();
        }

    }

    public int tailRange(long offset) throws java.io.IOException {
        String contentStr;
        long objectSize = 0;
        int tmpBytesRead = 0;
        int bytesRead = 0;
        byte[] content = new byte[EcsBufferedWriter.BUFFER_SIZE];
        System.err.println("JMC creating range with offset: " + offset);
        objectSize = this.getObjectListResult();
        if (objectSize <= offset) {
            return 0;
        }


        Range range = Range.fromOffsetLength(offset, EcsBufferedWriter.BUFFER_SIZE);
        if (objectSize < offset + EcsBufferedWriter.BUFFER_SIZE) {
            System.err.println("JMC changing up the range: " + (objectSize - offset));
            //range = Range.fromOffset(offset);
            range = Range.fromOffsetLength(offset, objectSize - offset);
        }

        InputStream is = this.s3Client.readObjectStream(this.bucket, this.key, range);
        //BufferedReader br = new BufferedReader(new InputStreamReader(is));
        BufferedInputStream br = new BufferedInputStream( is );

        //while((tmpContent = br.read()) != -1) {
        while((tmpBytesRead = br.read(content)) != -1) {
            bytesRead += tmpBytesRead;
            System.err.println("JMC number of bytesRead: " + bytesRead);
            //System.out.print(new String(content, "US-ASCII"));
            System.out.print(new String(content, 0, tmpBytesRead, "US-ASCII"));
            //System.out.print(new String(content, "UTF-8"));
            //contentStr = new String(content, 0, bytesRead, "UTF-8");
            //System.out.print(content);
        }
        return bytesRead;
    }


    public void tail() {
        int bytesRead = 0;

        //start from the beginning of the file unless it's specified to start as an offset from EOF
        long offset = 0;
        if (this.bytesFromEnd > 0) {
            offset = this.getObjectListResult() - this.bytesFromEnd;
        }
        try {
            if (this.s3Client == null) {
                throw (new RuntimeException("s3Client is null for some reason"));
            }
            while (true) {
                try {
                    bytesRead = tailRange(offset);
                    //if an exception was thrown, then this buffer increment line won't be hit
                    //offset += EcsBufferedWriter.BUFFER_SIZE;
                    System.err.println("JMC returned number of bytesRead: " + bytesRead);
                    offset += bytesRead;
                }
                catch(Exception e) {
                    System.err.println("probably a socket timeout");
                }
            }
        }

        catch(java.lang.NullPointerException e) {
            e.printStackTrace();
            System.err.println("The object may not exist");
        }
        //java.net.URISyntaxException
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public long getObjectListResult() {
        ListObjectsRequest lor = new ListObjectsRequest(this.bucket).withPrefix(this.key);
        ListObjectsResult res = this.s3Client.listObjects(lor);
        List<S3Object> objs = res.getObjects();
        //System.err.println("List objects length: " + objs.size());
        S3Object obj = objs.get(0);
        Long size = obj.getSize();
        //System.err.println(this.key + " size: " + size);
        return size;
    }


    @Override
    public void run() {
        //TODO - need possible option of bytes from EOF.
        // and will need to get length of file to do that.


        switch (this.action) {
            case TailFromEcs.TAILBYTE:
                this.tailByte();
                break;
            case TailFromEcs.TAIL:
                this.getObjectListResult();
                this.tail();
                break;
            default:
                break;

        }
    }
}
