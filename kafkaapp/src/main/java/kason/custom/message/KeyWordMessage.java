package kason.custom.message;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 * Created by zhangkai12 on 2017/7/31.
 */
public class KeyWordMessage  implements Encoder<KeyWord> {
    public KeyWordMessage(VerifiableProperties props){}

    @Override
    public byte[] toBytes(KeyWord keyWord) {
        System.out.println("keyWord object in encode");
        return keyWord.toString().getBytes();
    }
}
