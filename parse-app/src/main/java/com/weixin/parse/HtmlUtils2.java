package com.weixin.parse;


import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * Created by zhangkai12 on 2018/1/3.
 */
public class HtmlUtils2 {

    /**
     * 替换指定标签的属性和值
     * @param str 需要处理的字符串
     * @param tag 标签名称
     * @param tagAttrib 要替换的标签属性值
     * @param startTag 新标签开始标记
     * @param endTag  新标签结束标记
     * @return
     * @author huweijun
     * @date 2016年7月13日 下午7:15:32
     */
    public static String replaceHtmlTag(String str, String tag, String tagAttrib, String startTag, String endTag) {
        String regxpForTag = "<\\s*" + tag + "\\s+([^>]*)\\s*" ;
        String regxpForTagAttrib = tagAttrib + "=\\s*\"([^\"]+)\"" ;
        Pattern patternForTag = Pattern.compile (regxpForTag,Pattern. CASE_INSENSITIVE );
        Pattern patternForAttrib = Pattern.compile (regxpForTagAttrib,Pattern. CASE_INSENSITIVE );
        Matcher matcherForTag = patternForTag.matcher(str);
        StringBuffer sb = new StringBuffer();
        boolean result = matcherForTag.find();
        while (result) {
            StringBuffer sbreplace = new StringBuffer( "<"+tag+" ");
            Matcher matcherForAttrib = patternForAttrib.matcher(matcherForTag.group(1));
            if (matcherForAttrib.find()) {
                String attributeStr = matcherForAttrib.group(1);
                matcherForAttrib.appendReplacement(sbreplace, startTag + attributeStr + endTag);
            }
            matcherForAttrib.appendTail(sbreplace);
            matcherForTag.appendReplacement(sb, sbreplace.toString());
            result = matcherForTag.find();
        }
        matcherForTag.appendTail(sb);
        return sb.toString();
    }

    public static void main(String[] args) {
        try {
            String content = getFileContents("D:\\github_project\\git\\Spark2_1_0_Study\\parse-app\\src\\resources\\test.html");
            String newStr = replaceHtmlTag(content, "img", "data-src", "src=\"http://junlenet.com/", "\"");
            System.out.println("       替换后为:"+newStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static String getFileContents(String fileName) throws Exception {
        File theFile = new File(fileName);
        byte[] bytes = new byte[(int) theFile.length()];
        InputStream in = new FileInputStream(theFile);
        int m = 0, n = 0;
        while (m < bytes.length) {
            n = in.read(bytes, m, bytes.length - m);
            m += n;
        }
        in.close();

        return new String(bytes);
    }
}
