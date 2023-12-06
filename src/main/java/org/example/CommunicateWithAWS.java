package org.example;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CommunicateWithAWS {
    private final AmazonSQS sqs;
    private final AmazonS3 s3Client;
    private final S3Client s3;
    private final String bucketName = "picturesinput";

    public CommunicateWithAWS() {
        sqs = AmazonSQSClientBuilder.defaultClient();
        Region region = Region.US_EAST_1;

        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withRegion(String.valueOf(region))
                .build();

        this.s3 = S3Client.builder().region(Region.US_EAST_1).build();
    }

    public List<Message> receiveMessages(String sqsURL) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(sqsURL)
                .withWaitTimeSeconds(2)
                .withMaxNumberOfMessages(10);
        return sqs.receiveMessage(receiveMessageRequest).getMessages();
    }

    public Message receiveMessage(String sqsURL) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(sqsURL)
                .withWaitTimeSeconds(20)
                .withMaxNumberOfMessages(1);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        return messages.isEmpty() ? null : messages.get(0);
    }

    public void deleteMessage(Message message, String sqsURL) {
        sqs.deleteMessage(sqsURL, message.getReceiptHandle());
    }

    public void sendMessage(String message, String sqsURL) {
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(sqsURL)
                .withMessageBody(message)
                .withDelaySeconds(5);
        sqs.sendMessage(send_msg_request);
    }

    public void createSQS(String sqsName) {
        if (!s3Client.doesObjectExist(bucketName, sqsName)) {
            CreateQueueRequest create_request = new CreateQueueRequest(sqsName)
                    .addAttributesEntry("DelaySeconds", "60")
                    .addAttributesEntry("MessageRetentionPeriod", "86400");
            try {
                sqs.createQueue(create_request);
            } catch (AmazonSQSException e) {
                if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                    throw e;
                }
            }
        }
    }

    public void downloadFile(String key) {
        System.out.format("Downloading %s from S3 bucket %s...\n", key, bucketName);
        try {
            S3Object o = s3Client.getObject(bucketName, key);
            S3ObjectInputStream s3is = o.getObjectContent();
            FileOutputStream fos = new FileOutputStream(key);
            byte[] read_buf = new byte[1024];
            int read_len;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
            s3is.close();
            fos.close();
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public void uploadFile(String key, String filePath) {
        System.out.format("Uploading %s to S3 bucket %s...\n", filePath, bucketName);

        try {
            s3.putObject(PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .acl(ObjectCannedACL.PUBLIC_READ)
                        .build(), RequestBody.fromFile(new File(filePath)));
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }


    }

    public void uploadID(String key, String ID) {
        System.out.format("Uploading %s to S3 bucket %s... by %s\n", key, bucketName, ID);
        try {
            s3Client.putObject(bucketName, key, ID);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    public String getStringFromS3(String key) {
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(key)
                    .bucket(bucketName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();
            return new String(data, StandardCharsets.UTF_8);
        } catch (S3Exception e) {
            return "aaaaaaaaaaaaaa";
        }
    }

    public void deleteBucket() {

        ListObjectsRequest listRequest = ListObjectsRequest.builder()
                .bucket(bucketName).build();

        ListObjectsResponse listResponse = s3.listObjects(listRequest);
        List<software.amazon.awssdk.services.s3.model.S3Object> listObjects = listResponse.contents();

        List<ObjectIdentifier> objectsToDelete = new ArrayList<>();

        for (software.amazon.awssdk.services.s3.model.S3Object s3Object : listObjects) {
            objectsToDelete.add(ObjectIdentifier.builder().key(s3Object.key()).build());
        }

        DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(Delete.builder().objects(objectsToDelete).build())
                .build();

        DeleteObjectsResponse deleteObjectsResponse = s3.deleteObjects(deleteObjectsRequest);

        System.out.println("Objects deleted: " + deleteObjectsResponse.hasDeleted());

        DeleteBucketRequest request = DeleteBucketRequest.builder().bucket(bucketName).build();

        s3.deleteBucket(request);

        System.out.println("Bucket " + bucketName + " deleted.");
    }

}


