package com.weixin.parse;

/**
 * Created by IBM on 2018/1/2.
 */

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import javax.net.ssl.*;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;


public class WeiXinSpider {

    private ThreadPoolExecutor executorService;
    private List<String> imageLinks;

    private List<Future> imageFutures;
    private static final int corePoolSize = 5;
    private static final int maximumPoolSize = 10;
    private static final int keepAliveTime = 1;
    private static final int queueSize = 10;
    //for test
    private static final String saveImgPath="D:\\javen205-weixin_guide-master\\weixin_guide\\src\\main\\webapp\\wximages\\";
    //图片保存路径


    public WeiXinSpider() {
        imageFutures = new ArrayList<>();
        executorService = new
                ThreadPoolExecutor(corePoolSize,maximumPoolSize,keepAliveTime,
                TimeUnit.MINUTES, new
                ArrayBlockingQueue<Runnable>(queueSize));
        executorService.prestartAllCoreThreads();
    }

    public String getDoc(String url) throws IOException{
        //Document doc = Jsoup.connect("https://mp.weixin.qq.com/s/qLcSmXE0IoGti78qRV_RJQ").get();
        long start = System.currentTimeMillis();
        //https://mp.weixin.qq.com/s/-YC51PDSpIJX0r83OSdTew
        //Document doc = Jsoup.connect("https://mp.weixin.qq.com/s/788FjzoNBpSLkXAFO2Pr-w").get();
        //Document doc = Jsoup.connect("https://mp.weixin.qq.com/s/qLcSmXE0IoGti78qRV_RJQ").get();
        //Document doc = Jsoup.connect("https://mp.weixin.qq.com/s/-YC51PDSpIJX0r83OSdTew").get();
        Document doc = Jsoup.connect(url).get();
        //获取后缀为png和jpg的图片的元素集合
        //Elements pngs = doc.select("img[data-src~=(?i)\\.(png|jpe?g|gif)]");
        Elements pngs = doc.select("img[data-src]");
        System.out.println("size " + pngs.size());
        //遍历元素
        for(Element e : pngs){
            String src=e.attr("data-src");//获取img中的src路径

            System.out.println("src path " + src);
            //获取后缀名
            String imageLast = "png";
            if(src.contains("png")){
                imageLast = "png";
            }else if(src.contains("gif")){
                imageLast = "gif";
            }else if(src.contains("jpg")){
                imageLast = "jpg";
            }else if(src.contains("jpeg")){
                imageLast = "jpeg";
            }
            String imageName = UUID.nameUUIDFromBytes(src.getBytes()) +"." + imageLast;
            e.attr("data-src", "http://localhost:8080/wximages/" + imageName);
            //download
            imageFutures.add(executorService.submit(new DownLoadRunnable(src,saveImgPath + imageName)));
        }
        System.out.println("time1 cost " + (System.currentTimeMillis() - start));
        String parseHtml = doc.toString().replaceAll("data-src=", "src=");
        String htmlName = url.substring(url.lastIndexOf("/"));
        new Thread(new Runnable() {
            @Override
            public void run() {
                write(parseHtml, "D:\\javen205-weixin_guide-master\\weixin_guide\\src\\main\\webapp\\weixin\\" + htmlName +".html");
            }
        }).start();
        System.out.println("time1 cost " + (System.currentTimeMillis() - start));
        for(Future f : imageFutures){
            try {
                Object o = f.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        executorService.shutdown();
        //System.out.println(doc.toString());
        System.out.println("time cost " + (System.currentTimeMillis() - start));

        return parseHtml;
    }

    public void write(String htmlStr, String path){
        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(path));
            bufferedWriter.write(htmlStr);
        }catch (Exception e){

        }finally {
            if(bufferedWriter != null){
                try {
                    bufferedWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
    public static void main(String[] args) throws Exception {
        //String doc = new WeiXinSpider().getDoc("https://mp.weixin.qq.com/s/-YC51PDSpIJX0r83OSdTew");//调用方法
        // write(htmlFinal, "D:\\javen205-weixin_guide-master\\weixin_guide\\src\\main\\webapp\\weixin\\test2.html" );
        //System.out.println(doc);

        WeiXinSpider.test();
    }

    public static void test() throws Exception {
        long start = System.currentTimeMillis();
        BufferedReader bufferedReader = null;
        StringBuilder sb = new StringBuilder("");
        try {
            //URL url = new URL("https://mp.weixin.qq.com/s/-YC51PDSpIJX0r83OSdTew");

            // 创建SSLContext对象，并使用我们指定的信任管理器初始化
            URL url = new URL("http://blog.csdn.net/u011203602/article/details/48679579");
            HttpsURLConnection httpsConn = (HttpsURLConnection)url.openConnection();
            // 打开连接
            //URLConnection con = httpUrlConn.openConnection();
            //设置请求超时为5s
            //con.setConnectTimeout(5*1000);
            // 输入流
            InputStream is = httpsConn.getInputStream();

            bufferedReader = new BufferedReader(new InputStreamReader(
                    is, "UTF-8"));
            // 1K的数据缓冲
            //byte[] bs = new byte[1024];
            // 读取到的数据长度
            //int len;
            String string;
            while ((string = bufferedReader.readLine()) != null) {
                sb.append(string);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            bufferedReader.close();
        }

        System.out.println(sb.toString());
        System.out.println("time cost : " + (System.currentTimeMillis() - start));
    }

    static class MyX509TrustManager implements TrustManager, X509TrustManager {

        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        public void checkServerTrusted(X509Certificate[] certs, String authType) {
            //don't check
        }

        public void checkClientTrusted(X509Certificate[] certs, String authType) {
            //don't check
        }
    }
}