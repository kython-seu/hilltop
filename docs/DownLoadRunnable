package com.weixin.parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;

/**
 * Created by IBM on 2017/12/30.
 */
public class DownLoadRunnable implements Runnable {


    private Logger logger = LoggerFactory.getLogger(DownLoadRunnable.class);
    private String url;
    private String path;

    public DownLoadRunnable(String url, String path) {
        this.url = url;
        this.path = path;
    }

    @Override
    public void run() {
        try {
            download(url, path);
        } catch (Exception e) {
            logger.error("download image fail");
            e.printStackTrace();
        }
    }

    public void download(String _url,String path) throws Exception{
        try {
            // 构造URL
            URL url = new URL(_url);
            // 打开连接
            URLConnection con = url.openConnection();
            //设置请求超时为5s
            con.setConnectTimeout(5*1000);
            // 输入流
            InputStream is = con.getInputStream();

            // 1K的数据缓冲
            byte[] bs = new byte[1024];
            // 读取到的数据长度
            int len;
            // 输出的文件流
            File sf=new File(path);

            OutputStream os = new FileOutputStream(sf);
            // 开始读取
            while ((len = is.read(bs)) != -1) {
                os.write(bs, 0, len);
            }
            // 完毕，关闭所有链接
            os.close();
            is.close();


        } catch (IOException e) {

            //m_logger.error("DownLoadCutPhotoService.download error:"+e.getMessage());
        }finally {

        }
    }
}
