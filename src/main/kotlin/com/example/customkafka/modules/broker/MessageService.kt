package com.example.customkafka.modules.broker

import mu.KotlinLogging
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import java.util.*

private val logger = KotlinLogging.logger {}

@Service
class MessageService(
    val fileHandler: FileHandler,
    val configHandler: ConfigHandler,
    val restTemplate: RestTemplate
) {

    fun consume(id: Int): Message? {
        val partition = configHandler.getPartitionForConsumer(id) ?: return null
        val isLeader = configHandler.amILeader(partition.id)
        if (isLeader) {
            val message = fileHandler.readFile(partition) ?: return null
            //TODO update zookeeper and replicas about the new committed offset
            return message
        } else {
            configHandler.findLeaderBrokerId(partition.id).let { brokerId ->
                val conf = configHandler.getBrokerConfig(brokerId)
                val url = "http://${conf.host}:${conf.port}/message/consume/$id"
                val response = restTemplate.postForEntity(url, null, Message::class.java)
                return response.body
            }
        }
    }

    fun produce(key: String, message: String) {
        val partition = configHandler.getPartition(key)
        val messageObject = Message(key, message, Date(), partition)
        val isLeader = configHandler.amILeader(partition.id)
        if (isLeader) {
            fileHandler.addMessageToQueue(messageObject)
            configHandler.findReplicaBrokerIds(partition.id).forEach { brokerId ->
                val conf = configHandler.getBrokerConfig(brokerId)
                val url = "http://${conf.host}:${conf.port}/message/replica"
                val response = restTemplate.postForEntity(url, messageObject, String::class.java)
                // TODO what to do with the response
            }
        } else {
            configHandler.findLeaderBrokerId(partition.id).let { brokerId ->
                val conf = configHandler.getBrokerConfig(brokerId)
                val url = "http://${conf.host}:${conf.port}/message/produce"
                val request = MessageRequest(key, message)
                val response = restTemplate.postForEntity(url, request, String::class.java)
                // TODO what to do with the response
            }
        }
    }

    fun getReplicaMessages(message: Message) {
        if (configHandler.amIReplica(message.partition!!.id)) {
            fileHandler.addMessageToQueue(message)
        } else {
            logger.error { "I am not replica for partition ${message.partition}" }
        }
    }

    fun register(): Int {
        return configHandler.registerConsumer()
    }
}