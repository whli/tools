package mx.j2.recommend_spark.utils.redisTool;

import java.io.Closeable;
import org.apache.log4j.Logger;

/**
 * @author zhuowei
 */
public abstract class SerializeTranscoder {

    protected static Logger logger = Logger.getLogger(SerializeTranscoder.class);

    public abstract byte[] serialize(Object value);

    public abstract Object deserialize(byte[] in);

    public void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                logger.info("Unable to close " + closeable, e);
            }
        }
    }
}