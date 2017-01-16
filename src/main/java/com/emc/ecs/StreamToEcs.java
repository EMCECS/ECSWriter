/***********************************************************************
 * Copyright 2015 EMC Corporation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***********************************************************************/


package com.emc.ecs;

import com.emc.ecs.s3.sample.ECSS3Factory;
import com.emc.object.Range;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.request.ListObjectsRequest;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by conerj on 3/24/16.
 */
public class StreamToEcs implements Runnable  {
    private S3Client s3Client;
    private String bucket;
    private String key;
    private int action;

    protected static final int DOWRITE = 0;
    protected static final int DOAPPEND = 1;

    public StreamToEcs(S3Client s3Client, String bucket, String key, int action) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.key = key;
        this.action = action;
    }

    public void appendWrite() {
        long size = 0;
        // retrieve object key/value from user
        try {
            if (this.s3Client == null) {
                throw (new Exception("s3Client is null for some reason"));
            }
            if (this.getObjectListResult() < 0) {
                this.s3Client.putObject(this.bucket, this.key, "", null);
            }

            int tmpContent;
            String contentStr = "";
            StringBuilder contentBuilder = new StringBuilder();

            int loopCnt = 0;
            long currentAppendOffset = 0;
            long managedOffset = 0;

            try (BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) ) ){
                while((tmpContent = br.read()) != -1) {
                    contentBuilder.append((char) tmpContent);
                    if (loopCnt == EcsBufferedWriter.BUFFER_SIZE) {
                        loopCnt = 0;
                        //send what you've got and reset the buffer
                        contentStr = contentBuilder.toString();
                        currentAppendOffset = this.s3Client.appendObject(this.bucket, this.key, contentStr);

                        //currentAppendOffset is the offset where the append started
                        //which should be the same as the managedOffset before it's updated from this write
                        //this managedOffset is reset each time there's a mismatch
                        if ((currentAppendOffset != managedOffset) && (loopCnt > 0)) {
                            System.err.println("Offsets are not the same. Could be another writer to this file");
                            managedOffset = currentAppendOffset + contentStr.length();
                        }
                        else {
                            managedOffset += contentStr.length();
                        }
                        contentBuilder = new StringBuilder();

                    }
                    loopCnt++;
                }

                //you must've gotten a -1 EOF when reading the buffer
                //if there's a partial leftover buffer then send it.
                contentStr = contentBuilder.toString();
                if (contentStr.length() > 0) {
                    currentAppendOffset = this.s3Client.appendObject(this.bucket, this.key, contentStr);
                }
            }
        }
        catch(java.io.IOException ioe) {
            ioe.printStackTrace();
        }
        //java.net.URISyntaxException
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void putObjectWithRetry(byte[] content, Range range) {
        boolean retry = true;
        while (retry) {
            try {
                System.err.println("Entered putObjectWithRetry range: " + range.getFirst() + " - " + range.getLast());
                this.s3Client.putObject(this.bucket, this.key, range, (Object) content);
                System.err.println("putObject is done........................");
                retry = false;
            }
            catch(Exception e) {
                e.printStackTrace();
                System.err.println("putObjectWithRetry needs to retry");
            }
        }
    }

    public void rangeWrite() {
        // retrieve object key/value from user
        try {
            if (this.s3Client == null) {
                System.err.println("not sure why I need to call the ECSS3Factory again...");
                //this.s3 = ECSS3Factory.getS3Client();
                throw (new Exception("s3Client is null for some reason"));
            }

            System.err.println("Creating object: " + this.key);
            //this.s3Client.putObject(this.bucket, this.key, "blah2", null);
            this.s3Client.putObject(this.bucket, this.key, new byte[0], null);
            System.err.println("Just did the initial putObject...");

            byte[] content = new byte[EcsBufferedWriter.BUFFER_SIZE];
            int bytesRead = 0;
            long objectOffset = 0;

            System.err.println("Reading content and uploading...");

            //try (BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) ) ){
            try (BufferedInputStream br = new BufferedInputStream( System.in ) ){
                while((bytesRead = br.read(content,0, EcsBufferedWriter.BUFFER_SIZE)) != -1) {
                    //offset in ecs object and length to write
                    if (bytesRead > 0) {
                        Range range = Range.fromOffsetLength(objectOffset, bytesRead);
                        System.err.println("Going to putObject...");

                        //putObjectWithRetry(tmpStr.getBytes(), range);
                        putObjectWithRetry(content, range);

                        System.err.println("returned from putObjectWithRetry........................");
                        objectOffset += bytesRead;
                        content = new byte[EcsBufferedWriter.BUFFER_SIZE];
                    }
                    else {
                        System.err.println("JMC NO BYTES TO READ AND UPLOAD");
                    }
                }
            }
        }
        catch(java.io.IOException ioe) {
            ioe.printStackTrace();
        }
        //java.net.URISyntaxException
        catch(Exception e) {
            e.printStackTrace();
        }
    }


    public double getObjectListResult() {
        double size = -1;
        ListObjectsRequest lor = new ListObjectsRequest(this.bucket).withPrefix(this.key);
        ListObjectsResult res = this.s3Client.listObjects(lor);
        List<S3Object> objs = res.getObjects();
        if (objs.size() > 0) {
            //System.err.println("List objects length: " + objs.size());
            S3Object obj = objs.get(0);
            size = obj.getSize();
        }
        //System.err.println(this.key + " size: " + size);
        return size;
    }

    @Override
    public void run() {
        switch (this.action) {
            case StreamToEcs.DOWRITE:
                this.rangeWrite();
                break;
            case StreamToEcs.DOAPPEND:
                this.appendWrite();
                break;
            default:
                break;

        }
    }

}
