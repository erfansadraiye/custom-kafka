package com.example.customkafka.modules.broker

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.util.*

@Service
class MessageService(
    val fileWriter: FileWriter
) {

    fun sendMessage(key: String, message: String) {
        val partition = key.hashCode() % 1
        val messageObject = Message(key, message, Date(), partition)
        fileWriter.queues[partition]!!.add(messageObject)
    }
}
