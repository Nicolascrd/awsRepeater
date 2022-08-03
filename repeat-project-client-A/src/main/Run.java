package main;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class Run {

	public static void main(String[] args) {
		Regions REGION = Regions.US_EAST_1;
		AmazonS3 s3client;
		
		// user input 
	    String bucketName = "bucket61838";
	    String filePath = "/home/nicolascrd/Downloads/sales-2022-07-18.csv"; // should be changed to a GUI select
	    String queueUrl = "https://sqs.us-east-1.amazonaws.com/612940761661/Entry"; // maybe also change for user input
	    
	    
	    String fileName;
	    String[] arr = filePath.split("/");
	    
	    fileName = arr[arr.length - 1];
		try {
			s3client = AmazonS3ClientBuilder.standard()
                    .withRegion(REGION)
                    .build();
		} catch (Exception e) {
			System.err.println("Error building s3 client: "+ e);
			throw e;
		}
		
		Uploader up = new Uploader(s3client);
		Notifier notif = new Notifier();
		try {
			up.uploadFile(bucketName, fileName, filePath);
		} catch (AmazonServiceException e) {
			throw e;
		}
		System.out.println("Upload of sales csv file successful");
		
		try {
			notif.notifyWorker(bucketName, fileName, queueUrl);
		} catch (AmazonServiceException e) {
			throw e;
		}
	}


}
