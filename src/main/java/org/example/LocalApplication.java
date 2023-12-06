package org.example;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.io.*;
import java.util.List;

import software.amazon.awssdk.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.ec2.Ec2Client;

public class LocalApplication {
    public static void main(String[] args) {
        LocalApplicationGo localApplication = new LocalApplicationGo();
        localApplication.run(args);
    }

}

class LocalApplicationGo {

    private static String myID = "1";
    private static final Region region = Region.US_EAST_1;
    public static String SQSTaskUrl;
    public static String SQSResponseUrl;
    public static AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withCredentials(new ProfileCredentialsProvider())
            .withRegion(String.valueOf(region))
            .build();

    private CommunicateWithAWS communicateWithAWS;

    public void run(String[] args) {
        long startTime = System.currentTimeMillis();
        communicateWithAWS = new CommunicateWithAWS();
        communicateWithAWS.createSQS("EAndTSQSLocalApplicationTask");
        communicateWithAWS.createSQS("EAndTSQSLocalApplicationResponse");
        SQSTaskUrl = sqs.getQueueUrl("EAndTSQSLocalApplicationTask").getQueueUrl();
        SQSResponseUrl = sqs.getQueueUrl("EAndTSQSLocalApplicationResponse").getQueueUrl();

        init();

        if (args.length < 3) {
            System.out.println("Error not enough arguments");
            System.exit(1);
        }
        String inputFile = args[0];
        String outputFile = args[1];
        String ratio = args[2];
        boolean terminate = false;
        if (args.length == 4) {
            communicateWithAWS.uploadID("terminatedManager", myID);
            communicateWithAWS.sendMessage("terminate " + myID, SQSTaskUrl);
            terminate = true;
        }
        if (communicateWithAWS.getStringFromS3("terminatedManager").compareTo(myID) < 0) {
            System.out.println("Manager is already CLOSED");
            System.exit(0);
        }

        communicateWithAWS.uploadFile(myID, inputFile);
        communicateWithAWS.sendMessage("task " + myID, SQSTaskUrl);
        communicateWithAWS.sendMessage("n " + ratio, SQSTaskUrl);

        while (true) {
            List<Message> messages;
            while ((messages = communicateWithAWS.receiveMessages(SQSResponseUrl)).isEmpty()) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            for (Message message : messages) {
                String[] messageArr = message.getBody().split(" ");
                if (messageArr[0].equals(myID)) {
                    communicateWithAWS.downloadFile(message.getBody());
                    createHTML(message.getBody(), outputFile);
                    communicateWithAWS.deleteMessage(message, SQSResponseUrl);

                    long endTime = System.currentTimeMillis();
                    long min = ((endTime - startTime) /1000) /60;
                    String time = min < 10 ? "0"+min : String.valueOf(min);
                    long sec = ((endTime - startTime) /1000) %60;
                    time += ":" + (sec < 10 ? "0"+sec : String.valueOf(sec));
                    System.out.println("Total time: " + time);

                    System.exit(0);
                }
            }
        }

    }

    private void createHTML(String fileName, String outputFile) {
        try {
            FileWriter myWriter = new FileWriter(outputFile);
            myWriter.write("<!DOCTYPE html>\n" +
                    "<html>\n" +
                    "<head>\n" +
                    "<title>" + fileName + "</title>\n" +
                    "</head>\n" +
                    "<body>\n" +
                    "<h1> Welcome to " + myID + " html output </h1>\n");

            File file = new File(fileName);

            BufferedReader buffer = new BufferedReader(new FileReader(file));
            String line;
            int counter = 0;
            while ((line = buffer.readLine()) != null) {
                if (line.equals("*****")) {
                    line = buffer.readLine();
                    myWriter.write("<img src=\"" + line + "\"alt=\"can't load picture\" style=\"width:350px;\"><br>");
                    counter++;
                } else {
                    myWriter.write(line + "<br>");
                }
            }

            myWriter.write("total images processed: <b>" + counter + "</b><br>" +
                    "</body>\n </html>\n");
            myWriter.close();
            if(file.delete())
                System.out.println("html file created, response file deleted successfully");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    private void init() {
        try {
            String bucketName = "picturesinput";
            if (!s3Client.doesBucketExistV2(bucketName)) {
                // Because the CreateBucketRequest object doesn't specify a region, the
                // bucket is created in the region specified in the client.
                s3Client.createBucket(new CreateBucketRequest(bucketName));
                // Upload Manager to S3
                communicateWithAWS.uploadFile("Project", "Project.zip");
                // Creating Manager
                Ec2Client ec2 = Ec2Client.create();
                (new CreateEC2("Manager", ec2)).run();

            } else {
                myID = communicateWithAWS.getStringFromS3("ID");
            }
            String nextID = Integer.toString(Integer.parseInt(myID) + 1);
            communicateWithAWS.uploadID("ID", nextID);
        } catch (SdkClientException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it and returned an error response.
            e.printStackTrace();
        }// Amazon S3 couldn't be contacted for a response, or the client
// couldn't parse the response from Amazon S3.

    }
}

