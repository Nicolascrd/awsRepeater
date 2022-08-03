package ec2Worker;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.List;

public class SQSEntry {
	// receive messages from Client A
	public String url;
	private static final String QUEUE_NAME = "Entry";
	
	public SQSEntry(SqsClient sqsClient) {
		CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(QUEUE_NAME)
                .build();
		try {
			sqsClient.createQueue(createQueueRequest);
		} catch (SqsException e) {
		    throw e;
		}
		 // Get the URL of the queue
		GetQueueUrlResponse getQueueUrlResponse =
                sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build());
        url = getQueueUrlResponse.queueUrl(); 
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
	    		res = new String[2][messages.size()];
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
	
	public void DeleteSQSEntry(SqsClient sqsClient) {
		try {

            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(QUEUE_NAME)
                    .build();

            String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();

            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqsClient.deleteQueue(deleteQueueRequest);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
	}
}
