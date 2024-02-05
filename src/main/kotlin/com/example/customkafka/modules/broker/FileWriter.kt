package com.example.customkafka.modules.broker

import com.example.customkafka.server.objectMapper
import mu.KotlinLogging
import org.springframework.stereotype.Service
import java.io.File
import java.io.IOException
import java.util.*
import java.util.concurrent.PriorityBlockingQueue

val logger = KotlinLogging.logger {}

@Service
class FileWriter {


    val leaderPartitionList: List<Int> = listOf(0, 1)
    val replicaPartitionList: List<Int> = listOf(3, 4)

    val queues: Map<Int, PriorityBlockingQueue<Message>> = mapOf(
        0 to PriorityBlockingQueue(),
        1 to PriorityBlockingQueue(),
        3 to PriorityBlockingQueue(),
        4 to PriorityBlockingQueue()
    )

    init {

        try {
            for (i in leaderPartitionList) {
                val file = File("leader-messages-$i.txt")
                if (!file.exists()) {
                    file.createNewFile()
                }
            }

            for (i in replicaPartitionList) {
                val file = File("replica-messages-$i.txt")
                if (!file.exists()) {
                    file.createNewFile()
                }
            }


            leaderPartitionList.map { partition ->
                Thread {
                    while (true) {
                        val message = queues[partition]!!.poll()
                        if (message != null)
                            appendMessageToFile(message)
                        else
                            Thread.sleep(1000)
                    }
                }.start()
            }

            replicaPartitionList.map { partition ->
                Thread {
                    while (true) {
                        val message = queues[partition]!!.poll()
                        if (message != null)
                            appendMessageToFile(message)
                        else
                            Thread.sleep(1000)
                    }
                }.start()
            }

        } catch (e: IOException) {
            e.printStackTrace()
        }
    }

    @Synchronized
    fun appendMessageToFile(message: Message) {
        // read the file and append the message
        val fileName = if (leaderPartitionList.contains(message.partition)) {
            "leader-messages-${message.partition}.txt"
        } else {
            "replica-messages-${message.partition}.txt"
        }

        val file = File(fileName)
        val countLines = file.readLines().size
        val offset = countLines.toLong()
        message.offset = offset
        file.appendText(objectMapper.writeValueAsString(message) + "\n")
        logger.info { "Message appended to file: $message" }
    }
}


data class Message(
    val key: String? = null,
    val message: String? = null,
    val timestamp: Date? = null,
    val partition: Int? = null,
    var offset: Long? = null
) : Comparable<Message> {

    override fun compareTo(other: Message): Int {
        // Implement your comparison logic based on your requirements
        return timestamp!!.compareTo(other.timestamp)
    }

    override fun toString(): String {
        return "Message(key='$key', message='$message', timestamp=$timestamp, partition=$partition, offset=$offset)"
    }
}


//fun main(){
//
//    val fileWriter = FileWriter()
//    fileWriter.queues[0]!!.add(Message("key1", "message1", Date(), 0))
//    fileWriter.queues[0]!!.add(Message("key2", "message2", Date(), 0))
//    fileWriter.queues[0]!!.add(Message("key3", "message3", Date(), 0))
//
//    fileWriter.queues[1]!!.add(Message("key1", "message1", Date(), 1))
//
//    fileWriter.queues[3]!!.add(Message("key3", "message3", Date(), 3))
//
//}