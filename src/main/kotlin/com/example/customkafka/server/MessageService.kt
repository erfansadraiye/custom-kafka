package com.example.customkafka.server

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.util.*

@Service
class MessageService(
    @Value("\${kafka.partitions.num}")
    val numPartitions: Int,
    val fileWriter: FileWriter
) {

    fun sendMessage(key: String, message: String) {
        val partition = key.hashCode() % numPartitions
        val messageObject = Message(key, message, Date(), partition)
        fileWriter.queues[partition].add(messageObject)
    }
}
