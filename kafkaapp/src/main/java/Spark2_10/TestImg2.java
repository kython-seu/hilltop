package Spark2_10;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhangkai12 on 2017/12/29.
 */
public class TestImg2 {

    private String replaceImgSrc(String content,String replaceHttp){

        String result ="";

//       String patternStr="^.*<img\\s*.*\\s*src=\\\"(.*)\\\"\\s*.*>.*$";

//       String patternStr=".*?<img\\s*.*?\\s*src=\\\"(.*)\\\"\\s*.*?>.*";

//        String patternStr="^.*<img\\s*.*\\s*src=\\\"(.*?)\\\"\\s*.*>.*$";

//       String patternStr="<img(?:.*)src=(\"{1}|\'{1})([^\\[^>]+[gif|jpg|jpeg|bmp|bmp]*)(\"{1}|\'{1})(?:.*)>";

//       String patternStr="<img.*src=(.*?)[^>]*?>src=\"?(.*?)(\"|>|\\s+)";



//       String patternStr="(?i)<img[^>]*?src=\"([])\"";

//       String patternStr = "<img\\s+[^>]*?src="((\\w+?:\\/\\/|/)[^"]*?)"[^>]*?>";

        //         String patternStr = "<img\\s+[^>]*?src=\"((\\w+?:?//|\\/|\\w*)[^\"]*?)\"[^>]*?>";

        String patternStr = "<img\\s+[^>]*?src=[\"|\']((\\w+?:?//|\\/|\\w*)[^\"]*?)[\"|\'][^>]*?>";

        //Pattern pattern= Pattern.compile(patternStr);


        Pattern pattern = Pattern.compile("<img\\b[^>]*\\bsrc\\b\\s*=\\s*('|\")?([^'\"\n\r\f>]+(\\.jpg|\\.bmp|\\.eps|\\.gif|\\.mif|\\.miff|\\.png|\\.tif|\\.tiff|\\.svg|\\.wmf|\\.jpe|\\.jpeg|\\.dib|\\.ico|\\.tga|\\.cut|\\.pic)\\b)[^>]*>", Pattern.CASE_INSENSITIVE);

        Matcher matcher = pattern.matcher(content);

        //如果匹配到了img

        System.out.println("matcher.matches() == "+matcher.matches());

        if(matcher.matches()){

            result=content.replaceAll(matcher.group(1),(replaceHttp+matcher.group(1)));

            System.out.println(" result == "+result);

        }else{

            result =content;

        }

        return result;

    }

    public static void main(String[] args) {

        TestImg2 ss = new TestImg2();

        String content = "<p><img style=\"width:356px;height:163px;\" title=\"33\" alt=\"33\" src=\"http://localhost/WorkStation/attached/20110802/20110802131500_758.gif\" width=\"33\" height=\"3\"</p><p><img style=\"width:356px;height:163px;\" title=\"33\" alt=\"33\" src=\"/WorkStation/attached/20110802/20110802131500_758.gif\" width=\"33\" height=\"33\" /></p><p>&nbsp;</p><p><img style=\"width:356px;height:163px;\" title=\"33\" alt=\"33\" src=\"/WorkStation/attached/20110802/20110802131500_758.gif\" width=\"33\" height=\"3\"</p><p><img style=\"width:356px;height:163px;\" title=\"33\" alt=\"33\" src=\"/WorkStation/attached/20110802/20110802131500_758.gif\" width=\"33\" height=\"33\" /></p><p>&nbsp;</p>";

//     String content = "<p><img title=\"33\" alt=\"33\" align=\"left\" src=\"/WorkStation/attached/20110802/20110802173151_741.gif\" width=\"33\" height=\"33\" /></p><p>&nbsp;</p><p>&nbsp;</p><p>&nbsp;<img title=\"32\" alt=\"32\" src=\"/WorkStation/attached/20110802/20110802173215_520.gif\" width=\"33\" height=\"33\" /></p>" ;

        //ss.replaceImgSrc(content, "http://10.10.0.126:8088");
        //List<String> list = getImageSrc(content,"http://10.10.0.126:8088");

       /* for(String s : list){
            System.out.println(s);
        }*/
        getImageSrc(content,"http://10.10.0.126:8088");
    }

    public static List<String> getImageSrc(String htmlCode) {
        List<String> imageSrcList = new ArrayList<String>();
        Pattern p = Pattern.compile("<img\\b[^>]*\\bsrc\\b\\s*=\\s*('|\")?([^'\"\n\r\f>]+(\\.jpg|\\.bmp|\\.eps|\\.gif|\\.mif|\\.miff|\\.png|\\.tif|\\.tiff|\\.svg|\\.wmf|\\.jpe|\\.jpeg|\\.dib|\\.ico|\\.tga|\\.cut|\\.pic)\\b)[^>]*>", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(htmlCode);

        //System.out.println("matches ? " + m.matches());
        String quote = null;
        String src = null;
        //String result = htmlCode;
        while (m.find()) {
            quote = m.group(1);
            //result=htmlCode.replaceAll(quote,(replaceHttp+quote));
            // src=https://sms.reyo.cn:443/temp/screenshot/zY9Ur-KcyY6-2fVB1-1FSH4.png
            src = (quote == null || quote.trim().length() == 0) ? m.group(2).split("\\s+")[0] : m.group(2);
            imageSrcList.add(src);

        }
        return imageSrcList;
    }


    public static String getImageSrc(String htmlCode, String replaceHttp) {
        List<String> imageSrcList = new ArrayList<String>();
        Pattern p = Pattern.compile("<img\\b[^>]*\\bsrc\\b\\s*=\\s*('|\")?([^'\"\n\r\f>]+(\\.jpg|\\.bmp|\\.eps|\\.gif|\\.mif|\\.miff|\\.png|\\.tif|\\.tiff|\\.svg|\\.wmf|\\.jpe|\\.jpeg|\\.dib|\\.ico|\\.tga|\\.cut|\\.pic)\\b)[^>]*>", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(htmlCode);
        System.out.println("matches ? " + m.matches());
        String quote = null;
        String src = null;
        String result = htmlCode;
        int count = 0; // 图片个数
        while (m.find()) {
            count ++;
            quote = m.group(1);
            System.out.println("quote {}" + quote);
            System.out.println("size " + m.group(2).length());
            System.out.println("---" + m.group(2).split("\\s+")[0]);
            System.out.println("--------" + m.group(2));

            // src=https://sms.reyo.cn:443/temp/screenshot/zY9Ur-KcyY6-2fVB1-1FSH4.png
            //\\s表示   空格,回车,换行等空白符, +号表示一个或多个的意思
            src = (quote == null || quote.trim().length() == 0) ? m.group(2).split("\\s+")[0] : m.group(2);
            String imageName = src.substring(src.lastIndexOf("/"));
            System.out.println("src " + src + " name " + imageName);
            result=result.replace(src,(replaceHttp+imageName));


        }
        System.out.println(result);
        System.out.println("总共替换的图片个数 " + count);
        return result;
    }
}
