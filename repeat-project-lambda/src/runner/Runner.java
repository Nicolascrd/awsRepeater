package runner;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.ListTopicsRequest;
import software.amazon.awssdk.services.sns.model.ListTopicsResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.List;

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;


import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class Runner {
	private static final Region REGION = Region.US_EAST_1;
	private final String newFileName = "processedData.json";
	private SqsClient sqsClient;
	private SnsClient snsClient;
	private S3Client s3Client;
	private AmazonS3 amazS3;
	private String sqsEntry = "";
	private String sqsExit = "";
	private String snsExit = "";
	
	public void init() {
		System.out.println("init Lambda worker");
		this.sqsClient = SqsClient.builder().region(REGION).build();
		this.snsClient = SnsClient.builder().region(REGION).build();
		this.s3Client = S3Client.builder().region(REGION).build();	
		this.amazS3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
        try {
            ListTopicsRequest request = ListTopicsRequest.builder()
                   .build();

            ListTopicsResponse result = snsClient.listTopics(request);
            System.out.println("Status was " + result.sdkHttpResponse().statusCode() + "\n\nTopics\n\n" + result.topics());
            if (result.topics().size() != 2) {
            	System.err.println("SNS Topics number is " + result.topics().size() + " whereas should be 2");
            	System.exit(1);
            }
            System.out.println("looking for exit");
            for (int i = 0; i < result.topics().size(); i++) {
            	if (result.topics().get(i).topicArn().contains("Exit")) {
            		this.snsExit = result.topics().get(i).topicArn();
            	}
            }
        } catch (SnsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        if (this.snsExit.length() == 0) {
        	System.err.println("Could not find SNS Exit topic");
        	System.exit(1);
        }
        
        System.out.println("Listing available queues");
        
        ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().build();
        ListQueuesResponse listQueuesResponse = sqsClient.listQueues(listQueuesRequest);
        
        System.out.println("Available queues are : " + listQueuesResponse.queueUrls());
        
        if (listQueuesResponse.queueUrls().size() != 2) {
        	System.err.println("SQS Queues number is " + listQueuesResponse.queueUrls().size() + " whereas should be 2");
        	System.exit(1);
        }
        for (int i = 0; i < listQueuesResponse.queueUrls().size(); i++) {
            if (listQueuesResponse.queueUrls().get(i).contains("Entry")) {
            	this.sqsEntry = listQueuesResponse.queueUrls().get(i);
            }
            if (listQueuesResponse.queueUrls().get(i).contains("Exit")) {
            	this.sqsExit = listQueuesResponse.queueUrls().get(i);
            }
        }
        
        if (this.sqsExit.length() == 0) {
        	System.err.println("Could not find SQS Exit queue");
        	System.exit(1);
        }
        
        if (this.sqsEntry.length() == 0) {
        	System.err.println("Could not find SQS Entry queue");
        	System.exit(1);
        }
        
        System.out.println("Successfully initialized lambda worker");
	}
	
	public void run()  {
		
		System.out.println("Running Lambda worker");
		
		String[][] SQSEntries = retrieveSQSEntry(this.sqsClient, this.sqsEntry);
		if (SQSEntries.length > 1) {
			System.err.println("More than one element retrieved in the SQS Entry queue");
		}
		// we admin there is only one entry in SQS Queue
		
		CSVProcessor processor = new CSVProcessor(this.s3Client);
		Sender sender = new Sender(this.snsClient, this.sqsClient, this.sqsExit, this.snsExit);
		
		processor.processFile(SQSEntries[0][0], SQSEntries[0][1]);

		
		// generate file to upload to S3
		String stringifiedJson = "{\"totalNumberOfSales\": \"" +
				processor.totalNbSales + "\", \"totalAmountSold\": \"" +
				processor.totalAmountSold + "\", \"AvgSoldPerProduct\": " ;
		System.out.println(processor.avgSoldPerProduct.toString().replaceAll("=", ":"));
		
        Gson gson = new Gson();
        Type gsonType = new TypeToken<HashMap<String, Float>>(){}.getType();
        String gsonString = gson.toJson(processor.avgSoldPerProduct, gsonType);
        System.out.println("gsonString: " + gsonString);
		stringifiedJson += gsonString;	 
		stringifiedJson += "}";
		
		InputStream stream = new ByteArrayInputStream(stringifiedJson.getBytes(StandardCharsets.UTF_8));
		ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("application/json");
		PutObjectRequest req = new PutObjectRequest(SQSEntries[0][0], this.newFileName, stream, metadata);

        try {
        	this.amazS3.putObject(req);	
        } catch (AmazonServiceException e) {
        	throw e;
        }
        System.out.println("JSON file uploaded to S3 bucket");
        
		
		// send infos to SQS queue and notify SNS topic
		sender.send(processor, SQSEntries[0][0], SQSEntries[0][1], this.newFileName);
	}
	
	public String[][] retrieveSQSEntry(SqsClient sqsClient, String queueUrl) {
		// retrieve and delete by the way
				String[][] res;
				// list of messages, a message is a 2 element array with bucketName at [0] and fileName at [1]
				
				try {  
				ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
			                .queueUrl(queueUrl)
			                .maxNumberOfMessages(5)
			                .build();
			            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
			            int index = 0;
			    		res = new String[messages.size()][2];
			            messages.forEach(message -> {
			            	System.out.println("New message received by EC2 Worker");
			            	JsonObject json = JsonParser.parseString(message.body()).getAsJsonObject();
			            	res[index][0] = json.get("bucketName").getAsString();
			            	res[index][1] = json.get("fileName").getAsString();
			            });

			                for (Message message : messages) {
			                    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
			                        .queueUrl(queueUrl)
			                        .receiptHandle(message.receiptHandle())
			                        .build();
			                    sqsClient.deleteMessage(deleteMessageRequest);
			                }
			                System.out.println("Deleted " + messages.size() + " messages");
			            return res;
			        } catch (SqsException e) {
			            System.err.println(e.awsErrorDetails().errorMessage());
			            System.exit(1);
			        }
				return new String[0][];
	}
}
