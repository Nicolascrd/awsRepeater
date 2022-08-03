package ec2Worker;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.DeleteTopicRequest;
import software.amazon.awssdk.services.sns.model.DeleteTopicResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

import java.io.IOException;


public class RunEC2Worker {
	private static final Region REGION = Region.US_EAST_1;
	private final String lambdaArn = "arn:aws:lambda:us-east-1:612940761661:function:csvProcessor";
	private SqsClient sqsClient;
	private SnsClient snsClient;
	private SQSEntry entry;
	private SQSExit exit;
	private SNSEntry snsEn;
	private SNSExit snsEx;

	public void initWorker() {
		System.out.println("init EC2 worker");
		this.sqsClient = SqsClient.builder().region(REGION).build();
		this.snsClient = SnsClient.builder().region(REGION).build();
	}

	public void run() throws IOException {

		// Launch, create queues and topics

		System.out.println("launching EC2 worker");
		this.createQueuesAndTopics();

		// Entry processor
		String subEntry = this.snsEn.subscribe(this.snsClient, this.lambdaArn);
		if (subEntry != "") {
			System.out.println("Successfully added endpoint to SNS Entry, with ARN " + subEntry);
		} else {
			System.err.println("Error adding endpoint to SNS entry");
		}

	}

	public void createQueuesAndTopics() {
		try {
			this.entry = new SQSEntry(this.sqsClient);
		} catch (Exception e) {
			throw e;
		}
		System.out.println("SQS Queue Entry created with success with url " + entry.url);

		try {
			this.exit = new SQSExit(this.sqsClient);
		} catch (Exception e) {
			throw e;
		}
		System.out.println("SQS Queue Exit created with success with url " + exit.url);

		try {
			this.snsEx = new SNSExit(this.snsClient);
		} catch (Exception e) {
			throw e;
		}
		System.out.println("SNS Topic Exit created with success with ARN " + snsEx.topicArn);

		try {
			this.snsEn = new SNSEntry(this.snsClient);
		} catch (Exception e) {
			throw e;
		}
		System.out.println("SNS Topic Entry created with success with ARN " + snsEn.topicArn);
	}

	public void stop() {
		try {
			entry.DeleteSQSEntry(this.sqsClient);
		} catch (Exception e) {
			throw e;
		}
		System.out.println("SQS Queue Entry deleted successfully");

		try {
			exit.DeleteSQSEntry(this.sqsClient);
		} catch (Exception e) {
			throw e;
		}
		System.out.println("SQS Queue Exit deleted successfully");
		String status = "";
		try {
			DeleteTopicRequest request = DeleteTopicRequest.builder().topicArn(this.snsEn.topicArn).build();

			DeleteTopicResponse result = this.snsClient.deleteTopic(request);
			status = String.valueOf(result.sdkHttpResponse().statusCode());

		} catch (SnsException e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
		System.out.println("SNS Topic Entry deleted successfully with status " + status);

		try {
			DeleteTopicRequest request = DeleteTopicRequest.builder().topicArn(this.snsEx.topicArn).build();

			DeleteTopicResponse result = this.snsClient.deleteTopic(request);
			status = String.valueOf(result.sdkHttpResponse().statusCode());

		} catch (SnsException e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
		System.out.println("SNS Topic Exit deleted successfully with status " + status);
	}
}
