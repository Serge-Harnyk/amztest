import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.s3a.S3AInputStream;

import java.io.*;

/**
 * Created by serge on 18.02.16.
 */
public class AmazonEntryPoint {

    private static String bucketName = "serhiy.dremio.com";
    private static String key = "tmp/ltl.csv";

    public static void main(String[] args) throws IOException {
        AmazonS3Client s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        try {
            Long ts0 = System.currentTimeMillis();
            S3AInputStream s3Ais =
                    new S3AInputStream(bucketName, key, 0l, s3Client, new FileSystem.Statistics("s3a"));
            process(s3Ais);
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
        File f = new File("/tmp/dest.csv");
        FileWriter fr = new FileWriter(f);
        BufferedWriter br  = new BufferedWriter(fr);
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
            br.write(line);
            br.newLine();

        }
        br.close();

    }
}
