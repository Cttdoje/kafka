package kafka.examples;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ProducerInterceptorPrefix implements ProducerInterceptor<String,String> {

    private final AtomicInteger successCount = new AtomicInteger(0);
    private final  AtomicInteger failCount = new AtomicInteger(0);

    /**
     * 发送前拦截处理
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String,String> onSend(ProducerRecord<String,String> record) {
        String modifyValue = "prefix-"+record.value();
        return new ProducerRecord<>(record.topic(),record.partition(),record.timestamp(),
                record.key(),modifyValue,record.headers());
    }

    /**
     * 发送回调处理
     * @param metadata
     * @param exception
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception == null){
            successCount.incrementAndGet();
        }else {
            failCount.incrementAndGet();
        }
    }

    /**
     * 关闭客户端前调用
     */
    @Override
    public void close() {
        double ratio = successCount.get() /(successCount.get() + failCount.get());
        System.out.println(successCount.get());
        System.out.println(failCount.get());
        System.out.println("[发送成功率] "+ ratio);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
