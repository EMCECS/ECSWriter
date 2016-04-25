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


package com.emc.ecs.s3.sample;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.bean.AbstractVersion;
import com.emc.object.s3.bean.S3Object;

public class _99_DeleteBucket {

	public static void main(String[] args) throws Exception {
    	// create the ECS S3 Client
    	S3Client s3 = ECSS3Factory.getS3Client();

        // delete the demo bucket and all its content
        if (s3.getBucketVersioning(ECSS3Factory.S3_BUCKET).getStatus() != null) {
            for (AbstractVersion version : s3.listVersions(ECSS3Factory.S3_BUCKET, null).getVersions()) {
                s3.deleteVersion(ECSS3Factory.S3_BUCKET, version.getKey(), version.getVersionId());
            }
        } else {
            for (S3Object object : s3.listObjects(ECSS3Factory.S3_BUCKET).getObjects()) {
                s3.deleteObject(ECSS3Factory.S3_BUCKET, object.getKey());
            }
        }
        s3.deleteBucket(ECSS3Factory.S3_BUCKET);

    	// print bucket key/value and content for validation
    	System.out.println( String.format("deleted bucket [%s]",
    			ECSS3Factory.S3_BUCKET));
    }
}
