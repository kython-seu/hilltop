package Spark2_10;

/**
 * Created by zhangkai12 on 2017/12/29.
 */

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

public class PageSpider implements Runnable {

    HttpURLConnection httpUrlConnection;
    InputStream inputStream;
    BufferedReader bufferedReader;
    String url;

    public PageSpider() {

        try {
            url = "http://localhost:8081/view/share.jsp";
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            httpUrlConnection = (HttpURLConnection) new URL(url)
                    .openConnection(); // 创建连接
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Thread thread = new Thread(this);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void run() {
        // TODO Auto-generated method stub
        try {
            httpUrlConnection.setRequestMethod("GET");
        } catch (ProtocolException e) {
            e.printStackTrace();
        }

        try {
            httpUrlConnection.setUseCaches(true); // 使用缓存
            httpUrlConnection.connect(); // 建立连接
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter bufferedWriter = null;
        try {
            inputStream = httpUrlConnection.getInputStream(); // 读取输入流
            bufferedReader = new BufferedReader(new InputStreamReader(
                    inputStream, "UTF-8"));


            //bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:\\idea_workspace\\javen205-weixin_guide-master\\weixin_guide\\src\\main\\webapp\\view\\test.jsp"),"UTF-8"));
            bufferedWriter = new BufferedWriter(new FileWriter("D:\\\\idea_workspace\\\\javen205-weixin_guide-master\\\\weixin_guide\\\\src\\\\main\\\\webapp\\\\view\\\\test.jsp"));
            String str = "<%@ page language=\"java\" contentType=\"text/html; charset=UTF-8\"\n" +
                    "         pageEncoding=\"UTF-8\"%>";
            bufferedWriter.write(str);
            bufferedWriter.newLine();
            String string;
            while ((string = bufferedReader.readLine()) != null) {
                System.out.println(string); // 打印输出
                bufferedWriter.write(string);
                bufferedWriter.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bufferedReader.close();
                bufferedWriter.close();
                inputStream.close();
                httpUrlConnection.disconnect();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    public static void main(String[] args) {
        new PageSpider();
    }

}

