import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;

/**
 * Created by serge on 18.02.16.
 */
public class AmazonEntryPoint {

    private static String bucketName = "serhiy.dremio.com";
    private static String key = "tmp/ltl.csv";

    public static void main(String[] args) throws IOException {
        AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        try {
            System.out.println("Downloading an object");
            System.out.println(System.currentTimeMillis());
//            S3Object s3object = s3Client.getObject(new GetObjectRequest(
//                    bucketName, key));
//            System.out.println("Content-Type: " +
//                    s3object.getObjectMetadata().getContentType());
//            displayTextInputStream(s3object.getObjectContent());
            GetObjectRequest rangeObjectRequest = new GetObjectRequest(
                    bucketName, key);
            rangeObjectRequest.setRange(0, 10);
            S3Object objectPortion = s3Client.getObject(rangeObjectRequest);

            System.out.println("Printing bytes retrieved.");
            displayTextInputStream(objectPortion.getObjectContent());
            System.out.println(System.currentTimeMillis());
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which" +
                    " means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means" +
                    " the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    private static void displayTextInputStream(InputStream input)
            throws IOException {
        // Read one text line at a time and display.
        BufferedReader reader = new BufferedReader(new
                InputStreamReader(input));
        File f = new File("/tmp/dest.csv");
        FileWriter fr = new FileWriter(f);
        BufferedWriter br  = new BufferedWriter(fr);
        int count = 0;
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            count ++;
           // System.out.println(line);
            br.write(line);
            br.newLine();
            if(count % 1000000 == 0) {
                System.out.println(line);
            }
        }
        br.close();
        System.out.println(count);
    }
}
