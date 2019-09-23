package kafkatest

import com.typesafe.config.ConfigFactory
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.LongSerializer
import org.slf4j.LoggerFactory


object PaymentProducer {
    private val kafkaConf = ConfigFactory.load().getConfig("kafka")
    private val topic = kafkaConf.getString("topic")
    private val clientId = kafkaConf.getString("clientId")
    private val servers = kafkaConf.getStringList("servers").joinToString(",")
    private val producer = createProducer()
    private val logger = LoggerFactory.getLogger("MoneyKafkaProducer")

    private fun createProducer(): Producer<Long, AnalyticsMoney> {
        val props = mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to servers,
                ProducerConfig.ACKS_CONFIG to "1",
                ProducerConfig.RETRIES_CONFIG to "3",
                ProducerConfig.CLIENT_ID_CONFIG to clientId
        )
        return KafkaProducer(props, LongSerializer(), JsonObjectSerializer(AnalyticsMoney::class.java))
    }

    private fun makeRecord(user: AnalyticsMoney) = ProducerRecord<Long, AnalyticsMoney>(topic, user)

    fun send(data: AnalyticsMoney): RecordMetadata {
        logger.debug("Try to send to kafka: $data")
        val meta = producer.send(makeRecord(data)).get()
        producer.flush()
        return meta
    }

    fun sendAll(data: List<AnalyticsMoney>) {
        data.forEach {
            producer.send(makeRecord(it)).get()
        }
        producer.flush()
    }

    @JvmStatic
    fun main(args: Array<String>) {
        (0..20).forEach {
            GlobalScope.async {
                var i = 0L
                while (true) {
                    ++i
                    send(AnalyticsMoney(
                            "1234",
                            i,
                            "qqq",
                            "10.01.2001",
                            1000.0,
                            "89997772431",
                            "qqmber3000",
                            "89997773421",
                            1,
                            "any_type"
                    ))
                }
            }.start()
        }

        while (true) {
            Thread.sleep(1000)
        }
    }

}
