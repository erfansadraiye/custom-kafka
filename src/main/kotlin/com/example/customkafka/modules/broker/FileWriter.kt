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

    fun assignPartition() {
        leaderPartitionList = configHandler.getLeaderPartitionList()
        replicaPartitionList = configHandler.getReplicaPartitionList()
        queues = mapOf(
            *leaderPartitionList.map { it to PriorityBlockingQueue<Message>() }.toTypedArray(),
            *replicaPartitionList.map { it to PriorityBlockingQueue<Message>() }.toTypedArray()
        )

        try {
            for (i in leaderPartitionList) {
                val file = File(getLeaderPath(i))
                if (!file.exists()) {
                    file.parentFile.mkdirs()
                    file.createNewFile()
                }
            }

            for (i in replicaPartitionList) {
                val file = File(getReplicaPath(i))
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

    lateinit var leaderPartitionList: List<Int>
    lateinit var replicaPartitionList: List<Int>
    lateinit var queues: Map<Int, PriorityBlockingQueue<Message>>

    fun addMessageToQueue(message: Message) {
        queues[message.partition]!!.add(message)
    }

    private final fun getLeaderPath(partition: Int): String {
        return "${configHandler.getMyLogDir()}/leader-messages-$partition.txt"
    }

    private final fun getReplicaPath(partition: Int): String {
        return "${configHandler.getMyLogDir()}/replica-messages-$partition.txt"
    }


    @Synchronized
    fun appendMessageToFile(message: Message) {
        // read the file and append the message
        val fileName = if (leaderPartitionList.contains(message.partition)) {
            getLeaderPath(message.partition!!)
        } else {
            getReplicaPath(message.partition!!)
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

