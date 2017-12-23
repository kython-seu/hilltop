package kason.kafkamonitor.inter;

/**
 * Created by zhangkai12 on 2017/12/23.
 */
public interface JSONParserInter<T> {
    public T parse(String str);
    public boolean check();
}
