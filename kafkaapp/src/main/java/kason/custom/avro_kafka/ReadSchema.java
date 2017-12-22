package kason.custom.avro_kafka;

import java.io.*;

/**
 * Created by zhangkai12 on 2017/12/22.
 */
public class ReadSchema {

    public static String read(){
        File file = new File("avro.json");
        StringBuilder sb = new StringBuilder("");
        BufferedReader bufferedReader = null;
        try {
            InputStream resourceAsStream = ReadSchema.class.getClassLoader().getResourceAsStream("avro.json");
            bufferedReader = new BufferedReader(new InputStreamReader(resourceAsStream));

            if(bufferedReader != null){
                String data = "";
                while ((data = bufferedReader.readLine()) != null){
                    sb.append(data);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(bufferedReader != null){
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println(ReadSchema.read());
    }
}
