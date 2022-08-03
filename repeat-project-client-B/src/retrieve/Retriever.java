package retrieve;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.List;
import java.nio.file.Paths;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class Retriever {
	private SqsClient sqsClient;
	private S3Client s3Client;
	private String sqsExit;
	public String bucketName;
	public String oldFile;
	public String newFile;
	private static final Region REGION = Region.US_EAST_1;
	
	public void init() {
		System.out.println("init Client B");
		this.sqsClient = SqsClient.builder().region(REGION).build();
		this.s3Client = S3Client.builder().region(REGION).build();
		
		System.out.println("Listing available queues");
        
        ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().build();
        ListQueuesResponse listQueuesResponse = sqsClient.listQueues(listQueuesRequest);
        
        System.out.println("Available queues are : " + listQueuesResponse.queueUrls());
        
        if (listQueuesResponse.queueUrls().size() != 2) {
        	System.err.println("SQS Queues number is " + listQueuesResponse.queueUrls().size() + " whereas should be 2");
        	System.exit(1);
        }
        for (int i = 0; i < listQueuesResponse.queueUrls().size(); i++) {
            if (listQueuesResponse.queueUrls().get(i).contains("Exit")) {
            	this.sqsExit = listQueuesResponse.queueUrls().get(i);
            }
        }
        
        if (this.sqsExit.length() == 0) {
        	System.err.println("Could not find SQS Exit queue");
        	System.exit(1);
        }
        
        System.out.println("Successfully initialized Client B worker");
		
	}
	
	public boolean retrieveMessage() {
		System.out.println("Retrieving files");
		
		// res is a 2 element array with bucketName at [0] and oldfileName at [1] and newFileName at [2]
		List<Message> messages;
		try {  
		ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
	                .queueUrl(this.sqsExit)
	                .maxNumberOfMessages(1)
	                .build();
	            messages = this.sqsClient.receiveMessage(receiveMessageRequest).messages();
	            
	            if (messages.isEmpty()) {
	            	System.out.println("no message in exit queue");
	            	return false;
	            }
  
	            if (messages.size() > 1) {
	            	System.err.println("there should only be one message in exit queue");
	            	for (Message message : messages) {
		                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
		                    .queueUrl(this.sqsExit)
		                    .receiptHandle(message.receiptHandle())
		                    .build();
		                sqsClient.deleteMessage(deleteMessageRequest);
		            }
	            	System.exit(1);
	            }
	            
	            String body = messages.get(0).body();
            	JsonObject json = JsonParser.parseString(body).getAsJsonObject();
            	this.bucketName = json.get("bucketName").getAsString();
            	this.oldFile = json.get("oldFile").getAsString();
            	this.newFile = json.get("newFile").getAsString();
            	
            	System.out.printf("New message received by client B: bucketName is %s, original file name is %s, processed file name is %s", this.bucketName, this.oldFile, this.newFile);

	            for (Message message : messages) {
	                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
	                    .queueUrl(this.sqsExit)
	                    .receiptHandle(message.receiptHandle())
	                    .build();
	                sqsClient.deleteMessage(deleteMessageRequest);
	            }
	        System.out.println("Deleted 1 message");
	        
	    } catch (SqsException e) {
	        System.err.println(e.awsErrorDetails().errorMessage());
	        System.exit(1);
	    }
		return true;
	}
	
	public void retrieveFile(String oldFilePath, String newFilePath) {
		
		try {
			GetObjectRequest gor = GetObjectRequest.builder().bucket(this.bucketName).key(this.oldFile).build();
			this.s3Client.getObject(gor, Paths.get(oldFilePath));
		} catch (Exception e) {
			System.err.println("Could not write retrieved old file");
			System.exit(1);
		}
		
		try {
			GetObjectRequest gor = GetObjectRequest.builder().bucket(this.bucketName).key(this.newFile).build();
			this.s3Client.getObject(gor, Paths.get(newFilePath));
		} catch (Exception e) {
			System.err.println("Could not write retrieved new file");
			System.exit(1);
		}
		
		System.out.println("Wrote new and old file");
		
	}
}

//init Client B
//Listing available queues
//Available queues are : [https://sqs.us-east-1.amazonaws.com/612940761661/Entry, https://sqs.us-east-1.amazonaws.com/612940761661/Exit]
//Successfully initialized Client B worker
//Retrieving files
//New message received by client B: bucketName is bucket61838, original file name is sales-2022-07-18.csv, processed file name is processedData.jsonDeleted 1 message
//Wrote new and old file










