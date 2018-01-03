package com.weixin.parse;

import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebResponse;
import com.gargoylesoftware.htmlunit.html.*;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Tag;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangkai12 on 2018/1/2.
 */
public class Test {

    //private Logger logger = Log

    public static void main(String[] args) {
        grabImgUrl("");
    }

    public static List<String> grabImgUrl(String poiUid) {

       /* if (StringUtils.isBlank(poiUid)) {
            return null;
        }*/

        final String IMG_LIST_URL = "http://mp.weixin.qq.com/s?__biz=MjM5NDg2NjA4MQ==&mid=402566965&idx=1&sn=616fb1ffa9afc5acc3f4f2a210f6dd83&3rd=MzA3MDU4NTYzMw==&scene=6#rd";
        //String detailUrl = IMG_LIST_URL + poiUid;
        String detailUrl = IMG_LIST_URL;
        //log.info("grabImgUrl. detailUrl=" + detailUrl);

        final String DIV_ID = "photoContainer";
        final String TAG_IMG = "img";
        final String IMG_SRC = "data-src";

        try (final WebClient webClient = new WebClient()) {

            final HtmlPage page = webClient.getPage(detailUrl);
            //webClient.getOptions().setCssEnabled(false);
            //webClient.getOptions().setJavaScriptEnabled(false);
            System.out.println((page !=null) +"----" + (page.isHtmlPage()));
            try {
               // System.out.println("page " + new String(getPageToByte(page),"utf-8"));
                processImgSrc(new String(getPageToByte(page),"utf-8"));
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (page != null && page.isHtmlPage()) {

                //Thread.sleep(40000);  // 等待页面加载完成。

                DomNodeList<DomElement> elementsByTagName = page.getElementsByTagName(TAG_IMG);
                //final HtmlDivision div = page.getHtmlElementById(DIV_ID);

                //DomNodeList<HtmlElement> eleList = div.getElementsByTagName(TAG_IMG);

                List<String> imgUrlList = new ArrayList<String>();

                /*for (HtmlElement hele : eleList) {

                    String imgUrl = hele.getAttribute(IMG_SRC);

                    if (StringUtils.isNotBlank(imgUrl)) {
                        //log.info("imgUrl=" + imgUrl);
                        imgUrlList.add(imgUrl);
                    }
                }*/
                for (DomElement hele : elementsByTagName) {


                    String imgUrl = hele.getAttribute(IMG_SRC);

                    if (StringUtils.isNotBlank(imgUrl)) {
                        DomAttr attributeNode = hele.getAttributeNode(IMG_SRC);
                        attributeNode.setNodeValue("hahahha");
                        attributeNode.setValue("lolololo");
                        //log.info("imgUrl=" + imgUrl);
                        System.out.println("url   " + imgUrl);
                        imgUrlList.add(imgUrl);
                    }
                }


                return imgUrlList;
            }
        } catch (Exception e) {
            //log.error("grabImgUrl error.", e);
        }

        return null;
    }


    /**
     * 将 HtmlPage 转化为 byte
     * @param page
     * @return
     * @throws IOException
     */
    public static byte[] getPageToByte(HtmlPage page) throws IOException {
        byte[] responseContent = null;
        WebResponse webResponse = null;
        try {
            webResponse = page.getWebResponse();
            int status = webResponse.getStatusCode();
            // 读取数据内容
            if (status == 200) {
                InputStream bodyStream = webResponse.getContentAsStream();
                responseContent = ByteStreams.toByteArray(bodyStream);
                bodyStream.close();

                /*if (page.isHtmlPage()) {
                    // 等待JS执行完成
                    responseContent = page.asXml().getBytes();
                } else {
                    InputStream bodyStream = webResponse.getContentAsStream();
                    responseContent = ByteStreams.toByteArray(bodyStream);
                    bodyStream.close();
                }*/
            }
        } catch (IOException e) {
            throw new IOException(e);
        } finally {
            if (webResponse != null) {
                // 关闭响应流
                webResponse.cleanUp();
            }
        }
        return responseContent;
    }


    /**
     * 将文本中的相对地址转换成对应的绝对地址
     * @param content
     * @return
     */
    private static String processImgSrc(String content){
        Document document = Jsoup.parse(content);
        //document.setBaseUri(baseUrl);
        Elements elements = document.select("img[data-src]");

        for(Element el:elements){
            //el.
            System.out.println("=====" + el.getElementsByAttribute("data-src").text());
            Element src = el.tagName("src");
            String s = src.tagName();
            System.out.println("====" + s);
            String imgUrl = el.attr("data-src");
            if (imgUrl.trim().startsWith("/")) {
                el.attr("src", el.absUrl("src"));
            }
        }
        return document.html();
    }
}
