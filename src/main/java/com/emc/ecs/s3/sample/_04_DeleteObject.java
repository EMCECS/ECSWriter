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

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class _04_DeleteObject {

	public static void main(String[] args) throws Exception {
    	// create the ECS S3 Client
    	S3Client s3 = ECSS3Factory.getS3Client();

    	// retrieve the object value from user
        System.out.println( "Enter the object key:" );
        String key = new BufferedReader( new InputStreamReader( System.in ) ).readLine();
        
        // delete the object from the demo bucket
        s3.deleteObject(ECSS3Factory.S3_BUCKET, key);
        
        // print out key/value for validation
    	System.out.println( String.format("object [%s/%s] deleted",
    			ECSS3Factory.S3_BUCKET, key));
    }
}
