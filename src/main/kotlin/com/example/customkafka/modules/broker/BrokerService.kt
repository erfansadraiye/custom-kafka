package com.example.customkafka.modules.broker

import com.example.customkafka.modules.common.ClusterStatus
import com.example.customkafka.modules.common.PartitionDto
import io.micrometer.core.annotation.Timed
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import mu.KotlinLogging
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import java.util.*

private val logger = KotlinLogging.logger {}

@Service
class BrokerService(
    val fileHandler: FileHandler,
    val configHandler: ConfigHandler,
    val restTemplate: RestTemplate,
    val meterRegistry: MeterRegistry,
) {

    @Timed("broker.consume")
    fun consume(id: Int): Message? {
        return when (configHandler.status) {
            ClusterStatus.REBALANCING -> null
            ClusterStatus.MISSING_BROKERS -> throw Exception("Broker registry not finished.")
            ClusterStatus.NO_ZOOKEEPER -> throw Exception("No Zookeeper available")
            ClusterStatus.GREEN -> {
                val dto = configHandler.getPartitionForConsumer(id)
                //TODO do something better
                if (dto.partitionId == null) return Message("", "Invalid consumer Id", Date())
                if (dto.offset == null) return Message("", "All messages are consumed", Date())
                val isLeader = configHandler.amILeader(dto.partitionId)
                if (isLeader) {
                    val message = fileHandler.readFile(dto) ?: return null
                    dto.offset = dto.offset!! + 1
                    message.ack = "/ack/${dto.partitionId}/${dto.offset}"
                    return message
                } else {
                    configHandler.findLeaderBrokerId(dto.partitionId).let { brokerId ->
                        val conf = configHandler.getBrokerConfig(brokerId)
                        val url = "http://${conf.host}:${conf.port}/message/consume/$id"
                        val response = restTemplate.postForEntity(url, null, Message::class.java)
                        return response.body
                    }
                }
            }
        }
    }

    @Timed("broker.produce")
    fun produce(key: String, message: String) {
        when (configHandler.status) {
            ClusterStatus.MISSING_BROKERS -> throw Exception("Missing brokers")
            ClusterStatus.NO_ZOOKEEPER -> throw Exception("No Zookeeper!")
            else -> {}
        }
        val partition = configHandler.getPartition(key)
        val messageObject = Message(key, message, Date(), partition)
        val isLeader = configHandler.amILeader(partition)
        if (isLeader) {
            fileHandler.addMessageToQueue(messageObject)
            configHandler.findReplicaBrokerIds(partition).forEach { brokerId ->
                val conf = configHandler.getBrokerConfig(brokerId)
                val url = "http://${conf.host}:${conf.port}/message/replica"
                val response = restTemplate.postForEntity(url, messageObject, String::class.java)
                // TODO what to do with the response
            }
        } else {
            configHandler.findLeaderBrokerId(partition).let { brokerId ->
                val conf = configHandler.getBrokerConfig(brokerId)
                val url = "http://${conf.host}:${conf.port}/message/produce"
                val request = MessageRequest(key, message)
                val response = restTemplate.postForEntity(url, request, String::class.java)
                // TODO what to do with the response
            }
        }
        Counter.builder("total produced message count")
            .register(meterRegistry)
            .increment()
    }

    fun getReplicaMessages(message: Message) {
        if (configHandler.amIReplica(message.partition!!)) {
            fileHandler.addMessageToQueue(message)
        } else {
            logger.error { "I am not replica for partition ${message.partition}" }
        }
    }

    @Timed("broker.register")
    fun register(): Int {
        configHandler.status = ClusterStatus.REBALANCING
        val id = configHandler.callZookeeper("/zookeeper/consumer/register/${configHandler.baseConfig.brokerId}", null, String::class.java)
        configHandler.status = ClusterStatus.GREEN
        configHandler.reload()
        return id!!.toInt()
    }

    @Timed("broker.unregister")
    fun unregister(cId: String) {
        configHandler.status = ClusterStatus.REBALANCING
        configHandler.callZookeeper("/zookeeper/consumer/unregister/${configHandler.baseConfig.brokerId}/$cId", null, String::class.java)
        configHandler.status = ClusterStatus.GREEN
        configHandler.reload()
    }

    @Timed("broker.ack")
    fun ack(partition: Int, offset: Long) {
        configHandler.callZookeeper("/zookeeper/offset/commit", PartitionDto(partition, offset), String::class.java)
    }
}