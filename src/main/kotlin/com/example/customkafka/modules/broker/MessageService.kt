package com.example.customkafka.modules.broker

import mu.KotlinLogging
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import java.util.*

private val logger = KotlinLogging.logger {}

@Service
class MessageService(
    val fileWriter: FileWriter,
    val configHandler: ConfigHandler,
    val restTemplate: RestTemplate
) {

    fun sendMessage(key: String, message: String) {
        val partition = configHandler.getPartitionNumber(key)
        val messageObject = Message(key, message, Date(), partition)
        val isLeader = configHandler.amILeader(partition)
        if (isLeader) {
            fileWriter.addMessageToQueue(messageObject)
            configHandler.findReplicaBrokerIds(partition).forEach { brokerId ->
                val conf = configHandler.getBrokerConfig(brokerId)
                // Call the replica broker service with rest template
                val url = "http://${conf.host}:${conf.port}/message/replica"
                val response = restTemplate.postForEntity(url, messageObject, String::class.java)
                // TODO what to do with the response
            }
        } else {
            configHandler.findLeaderBrokerId(partition).let { brokerId ->
                val conf = configHandler.getBrokerConfig(brokerId)
                val url = "http://${conf.host}:${conf.port}/message/send"
                val response = restTemplate.postForEntity(url, messageObject, String::class.java)
                // TODO what to do with the response
            }
        }
    }

    fun getReplicaMessages(message: Message) {
        if (configHandler.amIReplica(message.partition!!)) {
            fileWriter.addMessageToQueue(message)
        } else {
            logger.error { "I am not replica for partition ${message.partition}" }
        }
    }
}