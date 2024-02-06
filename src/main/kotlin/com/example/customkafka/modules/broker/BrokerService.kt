package com.example.customkafka.modules.broker

import com.example.customkafka.modules.common.ClusterStatus
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import java.util.*

private val logger = KotlinLogging.logger {}

@Service
class BrokerService(
    val fileHandler: FileHandler,
    val configHandler: ConfigHandler,
    val restTemplate: RestTemplate,
    @Value("\${kafka.zookeeper.connect.url}")
    val zookeeperUrl: String,
) {

    fun consume(id: Int): Message? {
        return when (configHandler.status) {
            ClusterStatus.REBALANCING -> null
            ClusterStatus.MISSING_BROKERS -> throw Exception("Broker registry not finished.")
            ClusterStatus.NO_ZOOKEEPER -> throw Exception("No Zookeeper available")
            ClusterStatus.GREEN -> {
                val dto = configHandler.getPartitionForConsumer(id)
                val isLeader = configHandler.amILeader(dto.partitionId!!)
                if (isLeader) {
                    val message = fileHandler.readFile(dto) ?: return null
                    // these updates should take place after ack
                    restTemplate.postForEntity("$zookeeperUrl/zookeeper/offset/commit", dto, String::class.java).body
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
    }

    fun getReplicaMessages(message: Message) {
        if (configHandler.amIReplica(message.partition!!)) {
            fileHandler.addMessageToQueue(message)
        } else {
            logger.error { "I am not replica for partition ${message.partition}" }
        }
    }

    fun register(): Int {
        val id = restTemplate.postForEntity("$zookeeperUrl/zookeeper/consumer/register", null, String::class.java).body
        return id!!.toInt()
    }
}