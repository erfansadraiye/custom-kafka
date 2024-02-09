package com.example.customkafka.modules.zookeeper

import com.example.customkafka.modules.common.*
import com.example.customkafka.server.objectMapper
import jakarta.annotation.PostConstruct
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.web.client.*
import java.io.File
import java.util.Date
import java.util.concurrent.PriorityBlockingQueue
import kotlin.math.log

private val logger = KotlinLogging.logger {}

@Service
class ZookeeperService(
    @Value("\${kafka.partition-per-broker}")
    val partitionPerBroker: Int,
    val restTemplate: RestTemplate,
    @Value("\${kafka.zookeeper.slave.url}")
    val slaveUrl: String,
    @Value("\${kafka.zookeeper.master.url}")
    val masterUrl: String,
    @Value("\${kafka.zookeeper.master}")
    val isMaster: Boolean,
) {

    companion object {
        const val ZOOKEEPER_BROKER_PATH = "data/zookeeperBrokers.txt"
        const val ZOOKEEPER_PARTITION_PATH = "data/zookeeperPartitions.txt"
        const val ZOOKEEPER_PARTITION_PATTERN_PATH = "data/partitions/partition_{{index}}.txt"
        const val ZOOKEEPER_CONSUMER_PATH = "data/zookeeperConsumers.txt"
    }

    //TODO need to load from config
    var isHealthChecking = isMaster

    var leaders = mapOf<Int, MutableList<Int>>()
    var replications = mapOf<Int, MutableList<Int>>()

    var brokers = mutableListOf<BrokerConfig>()

    var consumers = mapOf<Int, MutableList<Int>>()

    var status = ClusterStatus.MISSING_BROKERS

    var partitions = mapOf<Int, PartitionData>()

    lateinit var partitionFileQueues: Map<Int, PriorityBlockingQueue<PartitionData>>

    val minimumBrokerCount = 2

    @PostConstruct
    fun setup() {
        logger.debug { "is master: $isMaster" }
        doPartitionConfigs()
        doBrokersConfigs()
        doConsumerConfigs()
    }

    @Scheduled(fixedRateString = "\${kafka.heartbeat-interval}", initialDelayString = "15000")
    fun checkBrokerHealth() {
        if (!isHealthChecking) {
            logger.debug { "Slave checking for master health..." }
            try {
                restTemplate.postForEntity("$masterUrl/zookeeper/health", null, String::class.java)
            } catch (e: Exception) {
                logger.warn { "Master is down. Slave doing health check." }
                isHealthChecking = true
            }
            return
        }
        logger.debug { "Starting broker health checking..." }
        var deadBroker: BrokerConfig? = null
        for (broker in brokers) {
            try {
                val res = restTemplate.postForEntity(
                    "http://${broker.host}:${broker.port}/message/ping",
                    null,
                    String::class.java).body
                if (res != "ok") {
                    deadBroker = broker
                }
            } catch (e: ResourceAccessException) {
                logger.error("Broker ${broker.brokerId} is dead. ${e.stackTrace}")
                deadBroker = broker
            } catch (e: Exception) {
                logger.error("Broker ${broker.brokerId} is dead. Unknown exception occurred. ${e.stackTrace}")
                deadBroker = broker
            }
        }
        brokers.remove(deadBroker)
        deadBroker?.let { selectedDeadBroker->
            status = ClusterStatus.MISSING_BROKERS
            reloadBrokerConfigs()
            val deadLeaders = selectedDeadBroker.config!!.leaderPartitionList
            deadLeaders.forEach { leader ->
                val candidates = brokers.filter { it.config!!.replicaPartitionList.contains(leader) }
                //TODO do something else?
                if (candidates.isEmpty()) throw Exception("No Replica found for partition $leader. data is lost!")
                logger.debug { "found replica for leader $leader in broker ${candidates.map { it.brokerId }}" }
                val selectedCandidate = candidates.random()
                selectedCandidate.config!!.apply {
                    replicaPartitionList.remove(leader)
                    leaderPartitionList.add(leader)
                    restTemplate.postForEntity(
                        "http://${selectedCandidate.host}:${selectedCandidate.port}/config/make-leader/$leader",
                        null,
                        String::class.java
                    )
                }
            }
            status = ClusterStatus.GREEN
            reloadBrokerConfigs()
        }
    }

    private fun doBrokersConfigs() {
        val file = File(ZOOKEEPER_BROKER_PATH)
        if (file.exists()) {
            brokers = if (file.length() != 0L) {
                val result = objectMapper.readValue(file.readText(), AllBrokers::class.java).brokers.toMutableList()
                status = ClusterStatus.GREEN
                result
            }
            else
                mutableListOf()
        } else {
            file.parentFile.mkdirs()
            file.createNewFile()
        }
    }

    private fun doPartitionConfigs() {
        val partitionsTemp = mutableMapOf<Int, PartitionData>()
        val brokerCount = minimumBrokerCount
        val partitionCount = brokerCount * partitionPerBroker
        partitionFileQueues = (0 until partitionCount).associateWith { PriorityBlockingQueue<PartitionData>() }.toMutableMap()
        repeat(partitionCount) { partition ->
            val file = File(getPartitionPath(partition))
            if (file.exists()) {
                partitionsTemp[partition] = objectMapper.readValue(file.readText(), PartitionData::class.java)
            } else {
                file.parentFile.mkdirs()
                file.createNewFile()
                partitionsTemp[partition] = PartitionData(partition, -1, -1, Date())
                file.writeText(objectMapper.writeValueAsString(partitionsTemp[partition]))
            }
            Thread {
                while (true) {
                    val data = partitionFileQueues[partition]!!.poll()
                    if (data != null)
                        updatePartitionData(data)
                    else
                        Thread.sleep(1000)
                }
            }.start()
        }
        partitions = partitionsTemp
        val file = File(ZOOKEEPER_PARTITION_PATH)
        if (file.exists()) {
            val configs = objectMapper.readValue(file.readText(), PartitionConfig::class.java)
            leaders = configs.leaderPartitionList
            replications = configs.replicaPartitionList
        } else {
//            partitions = (0 until partitionCount).map { PartitionData(it, -1, -1) }.associateBy { it.id }
            leaders = (0 until brokerCount).associateWith { mutableListOf() }
            replications = (0 until brokerCount).associateWith { mutableListOf() }
            partitions.values.forEachIndexed { index, p ->
                leaders[index % brokerCount]!!.add(p.id!!)
            }
            partitions.values.forEachIndexed { _, p ->
                val availableBrokers = leaders.filter { !it.value.contains(p.id) }.map { it.key }
                if (availableBrokers.isEmpty()) throw Exception("Replication factor cannot be bigger than broker count!")
                replications[availableBrokers.first()]!!.add(p.id!!)
            }
            val text = objectMapper.writeValueAsString(PartitionConfig(leaders, replications))
            //TODO inform slave zookeeper about new file
            file.parentFile.mkdirs()
            file.createNewFile()
            file.writeText(text)
        }
    }

    fun updateSlave() {
        if (isMaster) {
            try {
                restTemplate.postForEntity(
                    "$slaveUrl/zookeeper/update",
                    ZookeeperConfig(
                        AllBrokers(brokers),
                        PartitionConfig(leaders, replications),
                        ConsumerConfig(consumers),
                        status,
                        partitions
                    ),
                    String::class.java
                ).body
            } catch (e: ResourceAccessException) {
                logger.warn { "Slave is dead" }
            }
        }
    }

    private fun doConsumerConfigs() {
        val file = File(ZOOKEEPER_CONSUMER_PATH)
        if (file.exists()) {
            consumers = if (file.length() != 0L) {
                objectMapper.readValue(file.readText(), ConsumerConfig::class.java).consumers
            } else
                mutableMapOf()
        } else {
            file.parentFile.mkdirs()
            file.createNewFile()
        }
    }


    fun getConfigs(): AllConfigs {
        return AllConfigs(
            1,
            partitions.size,
            brokers,
            status,
        )
    }

    fun registerBroker(host: String, port: Int): Int {
        brokers.find { it.host == host && it.port == port }?.let {
            return it.brokerId!!
        }
        val id = (brokers.lastOrNull()?.brokerId ?: -1) + 1

        status = ClusterStatus.MISSING_BROKERS
        reloadBrokerConfigs()

        if (id > minimumBrokerCount - 1) {
            val pId = partitions.values.maxOf { it.id!! } + 1
            val range = pId  until pId + partitionPerBroker
            partitions += range.associateWith { PartitionData(it, -1, -1) }
            partitionFileQueues += range.associateWith { PriorityBlockingQueue<PartitionData>() }
            range.forEach { partition ->
                val file = File(getPartitionPath(partition))
                if (file.exists()) throw Exception("partition $partition file already exists in Zookeeper!")
                file.parentFile.mkdirs()
                logger.debug { "Making new partition file $partition" }
                file.createNewFile()
                file.writeText(objectMapper.writeValueAsString(PartitionData(partition, -1, -1)))
                Thread {
                    while (true) {
                        val data = partitionFileQueues[partition]!!.poll()
                        if (data != null)
                            updatePartitionData(data)
                        else
                            Thread.sleep(1000)
                    }
                }.start()
            }
            leaders += mapOf(id to range.toMutableList())
            range.forEach {
                //TODO maybe choose based on some property e.g. least number of partitions?
                val broker = brokers.random()
                replications[broker.brokerId]!!.add(it)
            }
            replications += mapOf(id to mutableListOf())
        }
        val config = BrokerConfig(id, host, port, MyConfig(leaders[id]!!, replications[id]!!))
        logger.debug { "registering with config: $config" }
        rebalanceConsumers()

        brokers.add(config)
        if (brokers.size >= 2) status = ClusterStatus.GREEN
        reloadBrokerConfigs(config.brokerId)
        val brokerFile = File(ZOOKEEPER_BROKER_PATH)
        brokerFile.writeText(objectMapper.writeValueAsString(AllBrokers(brokers)))
        val partitionFile = File(ZOOKEEPER_PARTITION_PATH)
        partitionFile.writeText(objectMapper.writeValueAsString(PartitionConfig(leaders, replications)))
        logger.debug { "New broker added with config: $config" }
        return id
    }

    fun registerConsumer(brokerId: Int): Int {
        if (status != ClusterStatus.GREEN) throw Exception("Cannot register consumer! cluster status is: ${status.name}")
        // notify other brokers
        status = ClusterStatus.REBALANCING
        reloadBrokerConfigs(brokerId)
        //TODO maybe wait for some time?
        val id = (consumers.keys.lastOrNull() ?: -1) + 1
        val newConsumers = consumers.mapValues { mutableListOf<Int>() } + mapOf(id to mutableListOf())
        rebalanceConsumers(newConsumers)
        status = ClusterStatus.GREEN
        reloadBrokerConfigs(brokerId)
        return id
    }

    fun unregisterConsumer(brokerId: Int, cId: Int) {
        if (status != ClusterStatus.GREEN) throw Exception("Cannot register consumer! cluster status is: ${status.name}")
        // notify other brokers
        status = ClusterStatus.REBALANCING
        reloadBrokerConfigs(brokerId)
        val newConsumers = consumers.filter { it.key != cId  }
        rebalanceConsumers(newConsumers)
        status = ClusterStatus.GREEN
        reloadBrokerConfigs(brokerId)
    }

    private fun rebalanceConsumers(newConsumers: Map<Int, MutableList<Int>> = consumers) {
        if (newConsumers.isEmpty()) return
        val cnt = newConsumers.keys.size
        partitions.keys.forEach {
            newConsumers[it % cnt]!!.add(partitions[it]!!.id!!)
        }
        consumers = newConsumers
        val file = File(ZOOKEEPER_CONSUMER_PATH)
        file.writeText(objectMapper.writeValueAsString(ConsumerConfig(consumers)))
        logger.debug { "Consumers: $consumers" }
    }

    private fun reloadBrokerConfigs(origin: Int? = null) {
        logger.debug { "all brokers: $brokers" }
        brokers
            .let {
                if (origin != null)
                    return@let it.filter { it.brokerId != origin }
                it
            }
            .forEach {
                while (true) {
                    try {
                        logger.debug { "reloading: ${it.brokerId} with origin: $origin" }
                        restTemplate.postForEntity(
                            "http://${it.host}:${it.port}/config/reload",
                            null,
                            String::class.java
                        )
                        break
                    }
                    catch (e: ResourceAccessException) {
                        logger.error { "reloading failed for broker ${it.brokerId}. Retrying..." }
                    }
                }
            }
    }

    fun updateLastOffset(id: Int, offset: Long) {
        logger.debug { "Updating last offset for partition: $id" }
        logger.debug { "Before update: $partitions" }
        val data = partitions[id]!!
        data.lastOffset = offset
        data.timestamp = Date()
        partitionFileQueues[id]!!.add(data)
        logger.debug { "After update: $partitions" }
    }

    fun updateCommitOffset(id: Int, offset: Long) {
        logger.debug { "Updating commit offset for partition: $id" }
        logger.debug { "Before update: $partitions" }
        val data = partitions[id]!!
        data.lastCommit = offset
        data.timestamp = Date()
        partitionFileQueues[id]!!.add(data)
        logger.debug { "After update: $partitions" }
    }

    @Synchronized
    private fun updatePartitionData(data: PartitionData) {
        val file = File(getPartitionPath(data.id!!))
        file.writeText(objectMapper.writeValueAsString(data))
    }

    private fun getPartitionPath(partition: Int) = ZOOKEEPER_PARTITION_PATTERN_PATH.replace("{{index}}", partition.toString())

    fun getPartitionOffsetForConsumer(id: Int): PartitionData? {
        if (id !in consumers.keys)
            return null
        val assignedPartitions = consumers[id]!!
        val partition = assignedPartitions
            .map { partitions[it]!! }
            .filter { (it.lastOffset!!) > (it.lastCommit!!) }
            .maxByOrNull { it.lastOffset!! - it.lastCommit!! }
        return partition ?: PartitionData(id)
    }

    fun updateConfig(body: ZookeeperConfig) {
        if (isMaster) return
        brokers = body.allBrokers.brokers.toMutableList()
        leaders = body.partitionConfig.leaderPartitionList
        replications = body.partitionConfig.replicaPartitionList
        consumers = body.consumersConfig.consumers
        status = body.status
        logger.debug { "New partitions received in slave: ${body.partitions}" }
        body.partitions.forEach { (partition, partitionData) ->
            val file = File(getPartitionPath(partition))
            if (!file.exists()) {
                file.parentFile.mkdirs()
                file.createNewFile()
                partitionFileQueues += mapOf(partition to PriorityBlockingQueue<PartitionData>())
                Thread {
                    while (true) {
                        val data = partitionFileQueues[partition]!!.poll()
                        if (data != null)
                            updatePartitionData(data)
                        else
                            Thread.sleep(1000)
                    }
                }.start()
            }
            file.writeText(objectMapper.writeValueAsString(partitionData))
        }
        val brokerFile = File(ZOOKEEPER_BROKER_PATH)
        brokerFile.writeText(objectMapper.writeValueAsString(AllBrokers(brokers)))
        val partitionFile = File(ZOOKEEPER_PARTITION_PATH)
        partitionFile.writeText(objectMapper.writeValueAsString(PartitionConfig(leaders, replications)))
        val consumerFile = File(ZOOKEEPER_CONSUMER_PATH)
        consumerFile.writeText(objectMapper.writeValueAsString(ConsumerConfig(consumers)))
    }
}

data class AllBrokers(
    val brokers: List<BrokerConfig> = listOf()
)


data class ZookeeperConfig(
    val allBrokers: AllBrokers,
    val partitionConfig: PartitionConfig,
    val consumersConfig: ConsumerConfig,
    val status: ClusterStatus,
    val partitions: Map<Int, PartitionData>,
)