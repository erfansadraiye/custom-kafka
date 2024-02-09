import com.example.customkafka.modules.broker.Message
import mu.KotlinLogging
import java.util.*
import kotlin.system.exitProcess
import org.springframework.web.client.RestTemplate

val ports = listOf(8081, 8082)
var REGISTERED = false
var ID: String? = null
private val log = KotlinLogging.logger {}


class ClientService(
    private val restTemplate: RestTemplate,
) {
    private fun getStringFromValue(bytes: ByteArray): String {
        return String(bytes)
    }

    fun push(key: String, value: Any) {
        try {
            val body = Message(
                key = key,
                message = if (value is String) value else getStringFromValue(value as ByteArray),
                timestamp = Date()
            )
            var url = "http://localhost:${ports[0]}/message/produce"
            restTemplate.postForEntity(url, body, String::class.java)

            url = "http://localhost:${ports[1]}/message/produce"
            restTemplate.postForEntity(url, body, String::class.java)
        } catch (e: Exception) {
            log.error("Could not push!")
        }
    }

    fun unregister(sig: Int = 0, mig: Int = 0) {
        try {
            var url = "http://localhost:${ports[0]}/message/unregister/$ID"
            restTemplate.postForEntity(url, "", String::class.java)

            url = "http://localhost:${ports[1]}/message/unregister/$ID"
            restTemplate.postForEntity(url, "", String::class.java)
        } catch (e: Exception) {
            log.error("Could not unregister!")
        }
    }

    fun register() {
        try {
            var url = "http://localhost:${ports[0]}/message/register"
            restTemplate.postForEntity(url, "", String::class.java)

            url = "http://localhost:${ports[1]}/message/register"
            restTemplate.postForEntity(url, "", String::class.java)
            REGISTERED = true
        } catch (e: Exception) {
            log.error("Could not register!")
        }
    }

    fun pull(): Pair<String, ByteArray>? {
        val content: String?
        val ack: String? = null

        if (!REGISTERED) {
            register()
        }
        try {
            var url = "http://localhost:${ports[0]}/message/consume/$ID"
            restTemplate.postForEntity(url, "", String::class.java)
            url = "http://localhost:${ports[1]}/message/consume/$ID"
            content = restTemplate.postForEntity(url, "", String::class.java).body!!


            val ackUrl = "http://localhost:${ports[0]}/message/ack/$ack"
            val result = restTemplate.postForEntity(ackUrl, "", String::class.java).body!!

            return Pair(content, result.toByteArray())

        } catch (e: java.net.SocketTimeoutException) {
            log.error("Could not pull!")
            return null
        }
    }


    fun subscribe(f: (String, ByteArray) -> Unit) {
        val temp = {
            val result = pull()
            f(result!!.first, result.second)
            exitProcess(0)
        }
        Thread(temp).start()
    }
}