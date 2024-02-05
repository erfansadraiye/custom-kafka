package com.example.customkafka.modules.broker

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.io.File
import java.io.IOException
import java.util.*
import java.util.concurrent.PriorityBlockingQueue

val logger = KotlinLogging.logger {}

@Service
class FileWriter(
    @Value("\${kafka.partitions.num}")
    private val numPartitions: Int
) {
    val queues = (0 until numPartitions).map { PriorityBlockingQueue<Message>() }


    init {

        try {
            for (i in 0 until numPartitions) {
                val file = File("messages-$i.txt")
                if (!file.exists()) {
                    file.createNewFile()
                }
            }

            (0 until numPartitions).map { partition ->
                Thread {
                    while (true) {
                        val message = queues[partition].poll()
                        appendMessageToFile(message)
                    }
                }.start()
            }
        } catch (e: IOException) {
            // Handle or log the exception
            e.printStackTrace()
        }
    }

    @Synchronized
    private fun appendMessageToFile(message: Message) {
        val file = File("messages-${message.partition}.txt")
        file.appendText("${message.key},${message.message},${message.timestamp},${message.partition}\n")
        logger.info { "Message written to file: $message" }
    }
}


data class Message(
    val key: String,
    val message: String,
    val timestamp: Date,
    val partition: Int
) : Comparable<Message> {

    override fun compareTo(other: Message): Int {
        // Implement your comparison logic based on your requirements
        return timestamp.compareTo(other.timestamp)
    }
}