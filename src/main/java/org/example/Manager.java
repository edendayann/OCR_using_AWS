package org.example;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import software.amazon.awssdk.services.ec2.Ec2Client;

import java.io.*;

import java.util.*;

public class Manager {

    public static void main(String[] args) {
        ManagerGo main = new ManagerGo();
        main.run();
    }

}

class ManagerGo {
    private int workingWorkers = 0;
    private int finishedTasks = 0;
    private int lastID;
    private int messages = 0;
    private int ratio = 10;
    private boolean terminate = false;

    private final CommunicateWithAWS communicateWithAWS = new CommunicateWithAWS();
    private final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

    private final String sqsLocalTasks = sqs.getQueueUrl("EAndTSQSLocalApplicationTask").getQueueUrl();
    private final String sqsLocalResponse = sqs.getQueueUrl("EAndTSQSLocalApplicationResponse").getQueueUrl();
    private String urlSQSManagerTask;
    private String urlSQSManagerResponse;

    private final CheckMessages checkLocalMessages = new CheckMessages(communicateWithAWS, sqsLocalTasks);
    private final Thread fromLocal = new Thread(checkLocalMessages);
    private CheckMessages checkWorkersMessages;
    private Thread fromWorker;
    private final HashMap<String, Task> tasks = new HashMap<>();
    private final Ec2Client ec2 = Ec2Client.create();
    private final List<CreateEC2> ec2List = new ArrayList<>();
    private final List<CreateEC2> ec2Stopped = new ArrayList<>();
    private String myInstanceID;

    public void run() {
        communicateWithAWS.createSQS("EAndTSQSManagerTask");
        communicateWithAWS.createSQS("EAndTSQSManagerResponse");
        urlSQSManagerTask = sqs.getQueueUrl("EAndTSQSManagerTask").getQueueUrl();
        urlSQSManagerResponse = sqs.getQueueUrl("EAndTSQSManagerResponse").getQueueUrl();

        fromLocal.start();

        checkWorkersMessages = new CheckMessages(communicateWithAWS, urlSQSManagerResponse);
        fromWorker = new Thread(checkWorkersMessages);
        fromWorker.start();

        Message toHandle, response;
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if ((toHandle = checkLocalMessages.getMessage()) != null) {
                String body = toHandle.getBody();
                String messageType = body.split(" ")[0];
                String id = body.split(" ")[1];

                switch (messageType) {
                    case "task":
                        handleTask(id);
                        break;
                    case "n":
                        ratio = Integer.parseInt(id);
                        break;
                    case "terminate":
                        terminate = true;
                        lastID = Integer.parseInt(id);
                        break;
                    case "instance":
                        myInstanceID = id;
                        break;
                }
            }

            if ((response = checkWorkersMessages.getMessage()) != null) {
                handleResponse(response);
                messages--;
            }

            if (terminate /*&& messages == 0*/ && finishedTasks == lastID) {
                cleanup();
                System.exit(0);
            }
        }
    }


    private void cleanup() {
        checkWorkersMessages.terminate();
        checkLocalMessages.terminate();
        for (CreateEC2 createEC2 : ec2List) {
            createEC2.terminate();
            workingWorkers--;
        }
        try{
            fromLocal.join();
            fromWorker.join();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        sqs.deleteQueue(urlSQSManagerTask);
        sqs.deleteQueue(sqsLocalTasks);
        sqs.deleteQueue(urlSQSManagerResponse);
        CreateEC2.terminate(myInstanceID, ec2);
        while (!(communicateWithAWS.receiveMessages(sqsLocalResponse)).isEmpty()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("terminating all..");
        sqs.deleteQueue(sqsLocalResponse);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        communicateWithAWS.deleteBucket();
    }

    public void handleTask(String body) {
        communicateWithAWS.downloadFile(body);
        File file = new File(body);
        int count = 0;

        try {
            BufferedReader buffer = new BufferedReader(new FileReader(file));
            String url;
            while ((url = buffer.readLine()) != null) {
                communicateWithAWS.sendMessage(body + "\n" + url, urlSQSManagerTask);
                count++;
            }
            // Delete the file
            if (file.delete())
                System.out.println("finished sending messages to workers, file deleted successfully");
        } catch (IOException e) {
            e.printStackTrace();
        }

        Task task = new Task(count, body + ".txt");
        tasks.put(body, task);
        messages += count;
        System.out.println("Number of Messages is " + messages);
        updateWorkers();
    }

    private void updateWorkers() {
        int num = (messages / ratio) - workingWorkers + ((messages % ratio == 0) ? 0 : 1);
        System.out.println("Number of Workers needed " + num);
        for (int i = 0; i < num && ec2List.size() < 10; i++) {
            workingWorkers++;
            boolean rebooted = false;
            while (!rebooted && !ec2Stopped.isEmpty()) {
                CreateEC2 toReboot = ec2Stopped.get(0);
                if (!(rebooted = toReboot.reboot())) { //failed to reboot
                    toReboot.terminate();
                    ec2List.remove(toReboot);
                }
                ec2Stopped.remove(0);
            }
            if (!rebooted) {
                CreateEC2 create = new CreateEC2("Worker " + ec2List.size(), ec2);
                ec2List.add(create);
                create.run();
            }
        }
        for (int i = 0; i > num; i--) {
            CreateEC2 toStop = ec2List.get(0);
            toStop.stop();
            ec2Stopped.add(toStop);
            workingWorkers--;
        }
    }

    private void handleResponse(Message response) {
        final String[] body = response.getBody().split("\n");
        String id = body[0];
        Task task = tasks.get(id);
        String[] newArray = Arrays.copyOfRange(body, 1, body.length);
        task.writeToFile(String.join("\n", newArray));
        if (task.unfinished == 0) {
            communicateWithAWS.uploadFile(id + " response", task.fileName);
            communicateWithAWS.sendMessage(id + " response", sqsLocalResponse);
            finishedTasks++;
            task.deleteFile();
            tasks.remove(id);
            updateWorkers();
        }
    }
}

class Task {
    public int unfinished;
    public String fileName;


    public Task(int messages, String fileName) {
        unfinished = messages;
        this.fileName = fileName;
        try {
            File file = new File(fileName);
            if (file.createNewFile()) {
                System.out.println("File created: " + file.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void deleteFile() {
        File file = new File(fileName);
        if (file.delete())
            System.out.println("finished task " + fileName + ", file deleted successfully");
    }


    public void writeToFile(String message) {
        try {
            FileWriter myWriter = new FileWriter(fileName, true);
            myWriter.write("*****\n" + message + "\n\n");
            myWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        unfinished--;
    }
}

class CheckMessages implements Runnable {
    private final CommunicateWithAWS communicateWIthAWS;
    private final List<Message> messages = new ArrayList<>();
    private final String sqsUrl;
    boolean terminate = false;

    public CheckMessages(CommunicateWithAWS communicateWIthAWS, String sqsUrl) {
        this.communicateWIthAWS = communicateWIthAWS;
        this.sqsUrl = sqsUrl;
    }

    public Message getMessage() {
        if (messages.isEmpty())
            return null;
        Message m = messages.get(0);
        messages.remove(0);
        return m;
    }

    public void terminate() {
        terminate = true;
    }

    @Override
    public void run() {
        while (!terminate) {
            try {
                Thread.sleep(1400);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Message currentMessage = communicateWIthAWS.receiveMessage(sqsUrl);
            if (currentMessage != null) {
                messages.add(currentMessage);
                communicateWIthAWS.deleteMessage(currentMessage, sqsUrl);
            }
        }
    }
}

