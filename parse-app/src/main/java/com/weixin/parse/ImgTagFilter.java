package com.weixin.parse;

/**
 * Created by zhangkai12 on 2018/1/3.
 */
import java.util.regex.*;
import java.io.*;

public class ImgTagFilter {
    private static final Pattern IMG_PATTERN = Pattern.compile(
            "<img(\\s+.*?)(?:src\\s*=\\s*(?:'|\")(.*?)(?:'|\"))(.*?)/>",
            Pattern.DOTALL|Pattern.CASE_INSENSITIVE);
    private static final Pattern CLASS_PATTERN = Pattern.compile(
            "class\\s*=\\s*(?:'|\")(.*?)(?:'|\")",
            Pattern.DOTALL|Pattern.CASE_INSENSITIVE);

    public static void main(String[] args) throws Exception {
        ImgTagFilter filter = new ImgTagFilter();
        String str = filter.getFileContents("img-parse-test.html");
        System.out.println(filter.rewriteImgTag(str));
    }

    public String rewriteImgTag(String str) {
        StringBuilder sb = new StringBuilder();
        Matcher m = IMG_PATTERN.matcher(str);
        int start = 0, end = 0;
        while (m.find()) { // find next match
            end = m.start();
            sb.append(str.substring(start, end));

            sb.append("<img");
            boolean classExists = false;
            for (int i = 1; i < m.groupCount() + 1; i++) {
                if (i == 2) { // image src
                    sb.append(" src=\"MY_IMAGE_SRC_REPLACEMENT\"");
                    continue;
                }

                Matcher mc = CLASS_PATTERN.matcher(m.group(i));
                if (mc.find()) {
                    classExists = true;
                    sb.append(m.group(i).substring(0, mc.start()));
                    sb.append(" class=\"" + mc.group(1) + " MY_CLASS\"");
                    sb.append(m.group(i).substring(mc.end()));
                }
                else {
                    sb.append(m.group(i));
                }
            }

            if (!classExists) {
                sb.append(" class=\"MY_CLASS\"");
            }

            start = m.end();
        }
        sb.append(str.substring(start, str.length()));
        return sb.toString();
    }

    String getFileContents(String fileName) throws Exception {
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
