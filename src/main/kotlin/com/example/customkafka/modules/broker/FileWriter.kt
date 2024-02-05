package com.example.customkafka.modules.broker

import com.example.customkafka.server.objectMapper
import mu.KotlinLogging
import org.springframework.stereotype.Service
import java.io.File
import java.io.IOException
import java.util.*
import java.util.concurrent.PriorityBlockingQueue

private val logger = KotlinLogging.logger {}

@Service
class FileWriter(
    private val configHandler: ConfigHandler
) {


    val leaderPartitionList: List<Int> = configHandler.getLeaderPartitionList()
    val replicaPartitionList: List<Int> = configHandler.getReplicaPartitionList()

    val queues: Map<Int, PriorityBlockingQueue<Message>> = mapOf(
        *leaderPartitionList.map { it to PriorityBlockingQueue<Message>() }.toTypedArray(),
        *replicaPartitionList.map { it to PriorityBlockingQueue<Message>() }.toTypedArray()
    )

    fun addMessageToQueue(message: Message) {
        queues[message.partition]!!.add(message)
    }

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
        return timestamp!!.compareTo(other.timestamp)
    }

    override fun toString(): String {
        return "Message(key='$key', message='$message', timestamp=$timestamp, partition=$partition, offset=$offset)"
    }
}

