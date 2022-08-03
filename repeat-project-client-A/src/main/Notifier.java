package main;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.regions.Region;

public class Notifier {
	SqsClient sqsClient;
	SnsClient snsClient;
	String snsEntryArn = "arn:aws:sns:us-east-1:612940761661:Entry";
	
	private final Region REGION = Region.US_EAST_1;
	Notifier() {
		this.sqsClient = SqsClient.builder().region(REGION).build();
		this.snsClient = SnsClient.builder().region(REGION).build();
	}
	
	private String messageFormatter(String bucketName, String fileName) {
		return "{\"bucketName\": \"" + bucketName + "\", \"fileName\": \"" + fileName + "\"}"; 
	}
	
	public void notifyWorker(String bucketName, String fileName, String queueUrl) {
		String mess = messageFormatter(bucketName, fileName);
		this.sqsClient.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody(mess).build());
		System.out.println("Worker notified with message " + mess);
		
        try {
            PublishRequest request = PublishRequest.builder()
                .topicArn(this.snsEntryArn)
                .message("{\"any\" : \"any\"}")
                .build();

            PublishResponse result = snsClient.publish(request);
            System.out.println(result.messageId() + " Message sent. Status is " + result.sdkHttpResponse().statusCode());

         } catch (SnsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
         }

	}	
}
