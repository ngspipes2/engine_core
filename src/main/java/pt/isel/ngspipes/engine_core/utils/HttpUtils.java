package pt.isel.ngspipes.engine_core.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import pt.isel.ngspipes.engine_core.implementations.ChronosJobStatusDto;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpUtils {

    public static void scheduleChronosJob(String url, String job) {

        try {

            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            OutputStreamWriter wr= new OutputStreamWriter(conn.getOutputStream());
            wr.write(job);
            wr.flush();
            wr.close();
            int responseCode = conn.getResponseCode();
            System.out.println(responseCode);
            System.out.println(conn.getResponseMessage());
            if(responseCode >= 400 && responseCode < 600)
                System.out.println(readStream(conn.getErrorStream()));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static boolean isChronosJobFinished(String url, String jobName) {
        url = url + "jobs/search?name=" + jobName;
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestMethod("GET");
            System.out.println("Getting status for: " + jobName);
            ChronosJobStatusDto chronosJobStatusDto = getChronosJobStatusDto(readStream(conn.getInputStream()));
            return chronosJobStatusDto.successCount > 0;
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return false;
    }


    private static String readStream(InputStream inputStream) throws IOException{
        BufferedReader br = null;
        String line;
        StringBuilder sb = new StringBuilder();

        try{
            br = new BufferedReader(new InputStreamReader(inputStream));

            while ((line = br.readLine()) != null)
                sb.append(line);

        } finally {
            if(br!=null)
                br.close();
        }

        return sb.toString();
    }

    private static ChronosJobStatusDto getChronosJobStatusDto(String content) throws IOException {
        return getObjectMapper(new JsonFactory()).readValue(content, ChronosJobStatusDto[].class)[0];
    }

    private static ObjectMapper getObjectMapper(JsonFactory factory) {
        return new ObjectMapper(factory);
    }
}
