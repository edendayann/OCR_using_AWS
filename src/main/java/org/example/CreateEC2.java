package org.example;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.Base64;
import java.util.List;

public class CreateEC2 implements Runnable {
    private String instanceID;
    private final String name;
    Ec2Client ec2;
    private final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

    CommunicateWithAWS communicateWithAWS = new CommunicateWithAWS();
    String urlSQSLocalApplicationTask = sqs.getQueueUrl("EAndTSQSLocalApplicationTask").getQueueUrl();

    public CreateEC2(String name, Ec2Client ec2) {
        this.ec2 = ec2;
        this.name = name;
    }

    public void run() {
        String main = name.equals("Manager") ? name : "Worker";

        RunInstancesRequest runRequest;
        IamInstanceProfileSpecification profileSpecification = IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build();

        String amiId = "ami-0b0dcb5067f052a63";
        runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T3_MICRO)
                .imageId(amiId)
                .maxCount(1)
                .minCount(1)
                .keyName("EdenAndTal")
                .iamInstanceProfile(profileSpecification)
                .userData(Base64.getEncoder().encodeToString(("#!/bin/bash\n" +
                        "sudo yum update -y\n" +
                        "sudo amazon-linux-extras install java-openjdk11 -y\n" +
                        "sudo yum install java-devel -y\n" +
                        "wget https://picturesinput.s3.amazonaws.com/Project\n" +
                        "unzip -P dsp231 Project\n" +
                        "java -cp target/assignment1.jar org.example." + main + "\n")
                        .getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        instanceID = response.instances().get(0).instanceId();
        if (main.equals("Manager"))
            communicateWithAWS.sendMessage("instance " + instanceID, urlSQSLocalApplicationTask);

        Tag tag = Tag.builder()
                .key(name)
                .value(name)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceID)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully started EC2 instance %s based on AMI %s\n",
                    instanceID, amiId);

        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public void stop() {
        System.out.println("stop instance " + instanceID);
        StopInstancesRequest request = StopInstancesRequest.builder()
                .instanceIds(instanceID)
                .build();

        ec2.stopInstances(request);
        System.out.printf("Successfully stopped instance %s\n", instanceID);
    }

    public boolean reboot(){
        System.out.println("reboot instance " + instanceID);
        try {
            RebootInstancesRequest request = RebootInstancesRequest.builder()
                    .instanceIds(instanceID)
                    .build();

            ec2.rebootInstances(request);
            System.out.printf("Successfully rebooted instance %s\n", instanceID);
            return true;
        } catch (Ec2Exception e) {
            return false;
        }
    }

    public static void terminate(String instanceID, Ec2Client ec2) {
        System.out.println("Terminating instance " + instanceID);
        try {
            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(instanceID)
                    .build();

            TerminateInstancesResponse response = ec2.terminateInstances(ti);
            List<InstanceStateChange> list = response.terminatingInstances();
            for (InstanceStateChange sc : list) {
                System.out.println("The ID of the terminated instance is " + sc.instanceId());
            }

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public void terminate() {
        System.out.println("terminate instance " + instanceID);
        try {
            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(instanceID)
                    .build();

            TerminateInstancesResponse response = ec2.terminateInstances(ti);
            List<InstanceStateChange> list = response.terminatingInstances();
            for (InstanceStateChange sc : list) {
                System.out.println("The ID of the terminated instance is " + sc.instanceId());
            }

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }


}
