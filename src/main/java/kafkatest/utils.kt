package kafkatest

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import java.io.UnsupportedEncodingException


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
data class AnalyticsMoney(
        @JsonProperty("id") val id: String,
        @JsonProperty("timestamp") val timestamp: Long,
        @JsonProperty("wallId") val wallId: String,
        @JsonProperty("pay_date") val pay_date: String,
        @JsonProperty("sum") val sum: Double,
        @JsonProperty("contact") val contact: String,
        @JsonProperty("login") val login: String?,
        @JsonProperty("phone") val phone: String?,
        @JsonProperty("provider") val provider: Int?,
        @JsonProperty("type") val type: String
)


class JsonObjectSerializer<T>(clazz: Class<T>) : Serializer<T> {
    private val mapper: ObjectMapper = jacksonObjectMapper()

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {

    }

    override fun serialize(topic: String?, data: T?): ByteArray {
        try {
            return mapper.writeValueAsBytes(data)
        } catch (e: UnsupportedEncodingException) {
            throw SerializationException("Error when serializing string to byte[] due to unsupported encoding")
        }

    }

    override fun close() {
        // nothing to do
    }
}

class JsonObjectDeserializer<T>(private val clazz: Class<T>) : Deserializer<T> {
    companion object {
        private val mapper: ObjectMapper = jacksonObjectMapper()
    }

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {

    }

    override fun deserialize(topic: String?, data: ByteArray?): T {
        try {
            return mapper.readValue(data, clazz)
        } catch (e: UnsupportedEncodingException) {
            throw SerializationException("Error when serializing string to byte[] due to unsupported encoding")
        }

    }

    override fun close() {
        // nothing to do
    }
}