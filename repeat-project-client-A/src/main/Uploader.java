package main;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.AmazonServiceException;


import java.io.File;


//import com.amazonaws.AmazonServiceException;
//import com.amazonaws.SdkClientException;
//import com.amazonaws.regions.Regions;
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3ClientBuilder;
//import com.amazonaws.services.s3.model.ObjectMetadata;
//import com.amazonaws.services.s3.model.PutObjectRequest;

public class Uploader {
	AmazonS3 s3client;
	public Uploader(AmazonS3 s3client) {
		this.s3client = s3client;
	}
	
	public void uploadFile (String bucketName, String fileObjKeyName, String filePath) {
		System.out.println("File upload request with bucket name " + bucketName + ", path " + filePath + ", name " + fileObjKeyName);
        PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(filePath));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("text/csv");
        request.setMetadata(metadata);
        try {
        	this.s3client.putObject(request);	
        } catch (AmazonServiceException e) {
        	throw e;
        }
	}
}
