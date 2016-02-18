import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.s3a.S3AInputStream;

import java.io.*;

/**
 * Created by serge on 18.02.16.
 */
public class AmazonEntryPoint {

    private static String bucketName = "serhiy.dremio.com";

    public static void main(String[] args) throws IOException {
        AmazonS3Client s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        String key = args[1];
        InputStream stream;
        try {
            Long ts0 = System.currentTimeMillis();
            if (args[0] == "aws") {
                System.out.println("Use Amazon SDK");
                S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));
                stream = s3object.getObjectContent();
            } else {
                System.out.println("Use Hadoop FS");
                stream = new S3AInputStream(bucketName, key, 0l, s3Client, new FileSystem.Statistics("s3a"));
            }
            process(stream);
            System.out.println(System.currentTimeMillis() - ts0);
        } catch (AmazonServiceException ase) {
            System.out.println("Error Message:    " + ase.getMessage());
        } catch (AmazonClientException ace) {
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    private static void process(InputStream input)
            throws IOException {
        // Read one text line at a time and display.
        BufferedReader reader = new BufferedReader(new
                InputStreamReader(input));
//        File f = new File("/tmp/dest.csv");
//        FileWriter fr = new FileWriter(f);
//        BufferedWriter br  = new BufferedWriter(fr);
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
//            br.write(line);
//            br.newLine();

        }
//        br.close();

    }
}
