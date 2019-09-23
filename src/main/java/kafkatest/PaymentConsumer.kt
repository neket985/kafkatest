package kafkatest

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.LongDeserializer
import org.slf4j.LoggerFactory


object PaymentConsumer {
    private val kafkaConf = ConfigFactory.load().getConfig("kafka")
    private val topic = kafkaConf.getString("topic")
    private val clientId = kafkaConf.getString("clientId")
    private val maxPollRecords = kafkaConf.getString("maxPollRecords")
    private val receiveBufferSizeBytes = kafkaConf.getString("receiveBufferSizeBytes")
    private val autoCommitIntervalMillis = kafkaConf.getString("autoCommitIntervalMillis")
    private val autoOffsetResetStrategy = kafkaConf.getString("autoOffsetResetStrategy")
    private val servers = kafkaConf.getStringList("servers").joinToString(",")

    private val logger = LoggerFactory.getLogger(PaymentConsumer::class.java)

    fun run() {
        val props = mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to servers,
                ConsumerConfig.GROUP_ID_CONFIG to clientId,
                ConsumerConfig.CLIENT_ID_CONFIG to clientId,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG to maxPollRecords,
                ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG to autoCommitIntervalMillis,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to autoOffsetResetStrategy,
                ConsumerConfig.RECEIVE_BUFFER_CONFIG to receiveBufferSizeBytes
        )
        val consumer = KafkaConsumer(props, LongDeserializer(), JsonObjectDeserializer(AnalyticsMoney::class.java))
        consumer.subscribe(listOf(topic))

        while (true) {
            val consumerRecords = consumer.poll(1000)
            consumerRecords.forEach { record ->
                val pay = record.value()
                println(pay.timestamp)
            }

            Thread.sleep(500)
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        run()
    }
}