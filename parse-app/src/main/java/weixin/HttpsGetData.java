package weixin;

/**
 * Created by zhangkai12 on 2018/1/3.
 */
import com.weixin.parse.WeiXinParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.cert.CertificateException;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

public class HttpsGetData {

    private static class TrustAnyTrustManager implements X509TrustManager {

        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[] {};
        }
    }

    private static class TrustAnyHostnameVerifier implements HostnameVerifier {
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    }

    String _url = "";

    public HttpsGetData(String url) {
        this._url = url;

    }

    public String download() throws Exception {
        long start = System.currentTimeMillis();
        StringBuilder result = new StringBuilder("");
        BufferedReader in = null;
        try {

            String urlStr = this._url;
            System.out.println("GET请求的URL为：" + urlStr);
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, new TrustManager[] { new TrustAnyTrustManager() }, new java.security.SecureRandom());
            URL realUrl = new URL(urlStr);
            // 打开和URL之间的连接
            HttpsURLConnection connection = (HttpsURLConnection) realUrl.openConnection();
            // 设置https相关属性
            connection.setSSLSocketFactory(sc.getSocketFactory());
            connection.setHostnameVerifier(new TrustAnyHostnameVerifier());
            connection.setDoOutput(true);

            // 设置通用的请求属性
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 建立实际的连接
            connection.connect();

            // 定义 BufferedReader输入流来读取URL的响应
            in = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"));
            String line;
            while ((line = in.readLine()) != null) {
                result.append(line);
                result.append("\n");
            }
            //System.out.println("获取的结果为：\n" + result);
        } catch (Exception e) {
            System.out.println("发送GET请求出现异常！" + e);
            // e.printStackTrace();
            throw e;
        }
        // 使用finally块来关闭输入流
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                // e2.printStackTrace();
                throw e2;
            }
        }

        System.out.println("download this page cost time " + (System.currentTimeMillis() - start));
        return result.toString();

    }




    public static void main(String[] args) {
        String url = "https://mp.weixin.qq.com/s/qLcSmXE0IoGti78qRV_RJQ";
        try {
            String download = new HttpsGetData(url).download();
            new WeiXinParser(url).replaceHtmlTag(download,"img", "data-src", "src=\"http://localhost:8080/wximages/", "\"");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}