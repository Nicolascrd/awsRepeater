package ec2Worker;

import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

public class SNSExit {
	// send notifications to Client B
	private final String topicName = "Exit";
	public String topicArn;
	public SNSExit(SnsClient snsClient) {
		CreateTopicResponse result = null;
	    try {
	        CreateTopicRequest request = CreateTopicRequest.builder()
	                .name(topicName)
	                .build();

	        result = snsClient.createTopic(request);
	        this.topicArn = result.topicArn();
	    } catch (SnsException e) {
	        System.err.println(e.awsErrorDetails().errorMessage());
	        System.exit(1);
	    }
	}
}
