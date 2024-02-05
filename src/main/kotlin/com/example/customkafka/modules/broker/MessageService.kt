package com.example.customkafka.modules.broker

import mu.KotlinLogging
import org.springframework.stereotype.Service
import java.util.*

private val logger = KotlinLogging.logger {}

@Service
class MessageService(
    val fileWriter: FileWriter,
    val configHandler: ConfigHandler
) {

    fun sendMessage(key: String, message: String): Boolean {
        val partition = configHandler.getPartitionNumber(key)
        val messageObject = Message(key, message, Date(), partition)
        val isLeader = configHandler.amILeader(partition)
        return if (isLeader) {
            fileWriter.addMessageToQueue(messageObject)
            configHandler.findReplicaBrokerIds(partition).forEach { brokerId ->
                //TODO send replica message to replica with feign client
            }
            true
        } else {
            configHandler.findLeaderBrokerId(partition).let { brokerId ->
                //TODO send message to leader
            }
            true
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
