import java.net.*;
import java.io.*;

public class URLConnectionReader {
    public static void main(String[] args) throws Exception {
        URL oracle = new URL("http://master.cdh.queens:19888/jobhistory/logs/slave3:8041/container_1404361347212_0001_01_000005/attempt_1404361347212_0001_m_000000_0/ubuntu/syslog/?start=0");
        URLConnection yc = oracle.openConnection();
        BufferedReader in = new BufferedReader(new InputStreamReader(
                                    yc.getInputStream()));
        String inputLine;
        while ((inputLine = in.readLine()) != null )
        	if (inputLine.contains("hdfs"))
        	{
        		inputLine = inputLine.replaceAll("\\+", " +");
        		String []fileParts = inputLine.split(":8020");
        		String []fileNames = fileParts[1].split(":");
        		String []startAndEnd = fileNames[1].split(" +");
        		System.out.println("The file name is " + fileNames[0] + " start " + startAndEnd[0] + " end " + startAndEnd[1]);
        		System.out.println(inputLine);
        	}
        in.close();
    }
}