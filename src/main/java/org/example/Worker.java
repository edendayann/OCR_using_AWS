package org.example;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.asprise.ocr.Ocr;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Worker {
    public static void main(String[] args) {
        WorkerGo main = new WorkerGo();
        main.run();
    }

}

class WorkerGo {
    public static AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
    private final CommunicateWithAWS communicateWithAWS = new CommunicateWithAWS();

    private final String urlSQSManagerTask = sqs.getQueueUrl("EAndTSQSManagerTask").getQueueUrl();
    private final String urlSQSManagerResponse = sqs.getQueueUrl("EAndTSQSManagerResponse").getQueueUrl();



    public void run() {
        while (true) {
            Message message = communicateWithAWS.receiveMessage(urlSQSManagerTask);
            while (message == null) {
                message = communicateWithAWS.receiveMessage(urlSQSManagerTask);
            }
            String[] messageBody = message.getBody().split("\n");
            String response;
            if(messageBody.length < 2)
                response = "input not valid";
            else {
                String url = messageBody[1];
                response = url + "\n";
                String[] urlArray = url.split("/");
                String imageName = urlArray[urlArray.length - 1];
                try {
                    saveImage(url, imageName);
                    response += OCR(imageName);
                } catch (IOException e) {
                    response += e.getMessage();
                }
            }
            communicateWithAWS.sendMessage(messageBody[0] +"\n" + response, urlSQSManagerResponse);
            communicateWithAWS.deleteMessage(message, urlSQSManagerTask);
        }
    }

    private String OCR(String image) {
        Ocr.setUp();
        Ocr ocr = new Ocr(); // create a new OCR engine
        ocr.startEngine("eng", Ocr.SPEED_FASTEST); // English
        File[] files = new File[]{new File(image)};
        String response = ocr.recognize(files,
                Ocr.RECOGNIZE_TYPE_ALL, Ocr.OUTPUT_FORMAT_PLAINTEXT); // PLAINTEXT | XML | PDF | RTF
        ocr.stopEngine();
        if(files[0].delete())
            System.out.println("finished OCR on image, file deleted successfully");
        return response;
    }

    public void saveImage(String imageUrl, String destinationFile) throws IOException {
        URL url = new URL(imageUrl);
        InputStream is = url.openStream();
        OutputStream os = Files.newOutputStream(Paths.get(destinationFile));
        byte[] b = new byte[2048];
        int length;

        while ((length = is.read(b)) != -1) {
            os.write(b, 0, length);
        }

        is.close();
        os.close();
    }

}
