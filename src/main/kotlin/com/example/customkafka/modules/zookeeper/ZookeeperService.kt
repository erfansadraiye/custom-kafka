package com.example.customkafka.modules.zookeeper

import com.example.customkafka.modules.common.*
import com.example.customkafka.server.objectMapper
import io.micrometer.core.annotation.Timed
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.web.client.ResourceAccessException
import org.springframework.web.client.RestTemplate
import java.io.File
import java.util.*
import java.util.concurrent.PriorityBlockingQueue

private val logger = KotlinLogging.logger {}

@Service
class ZookeeperService(
    @Value("\${kafka.partition-per-broker}")
    val partitionPerBroker: Int,
    val restTemplate: RestTemplate,
    val meterRegistry: MeterRegistry,
    @Value("\${kafka.zookeeper.slave.url}")
    val slaveUrl: String,
    @Value("\${kafka.zookeeper.master.url}")
    val masterUrl: String,
    @Value("\${kafka.zookeeper.master}")
    val isMaster: Boolean,
) {

    companion object {
        const val ZOOKEEPER_BROKER_PATH = "data/zookeeperBrokers.txt"
        const val ZOOKEEPER_OUT_OF_SYNC_BROKER_PATH = "data/zookeeperOutOfSyncBrokers.txt"
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

    val outOfSyncBrokers = mutableListOf<BrokerConfig>()

    val brokerRegistryQueue = PriorityBlockingQueue<BrokerRegistryDto>()

    @PostConstruct
    fun setup() {
        logger.debug { "is master: $isMaster" }
        doPartitionConfigs()
        doBrokersConfigs()
        doConsumerConfigs()
    }

    @Scheduled(fixedRateString = "\${kafka.heartbeat-interval}", initialDelayString = "\${kafka.heartbeat-delay}")
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
            outOfSyncBrokers.add(deadBroker)
            File(ZOOKEEPER_OUT_OF_SYNC_BROKER_PATH).writeText(objectMapper.writeValueAsString(AllBrokers(outOfSyncBrokers)))
            val deadLeaders = selectedDeadBroker.config!!.leaderPartitionList
            leaders = leaders.minus(selectedDeadBroker.brokerId!!)
            replications = replications.minus(selectedDeadBroker.brokerId)
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
//                leaders[selectedCandidate.brokerId]!!.add(leader)
//                replications[selectedCandidate.brokerId]!!.remove(leader)
            }
            File(ZOOKEEPER_BROKER_PATH).writeText(objectMapper.writeValueAsString(AllBrokers(brokers)))
            File(ZOOKEEPER_PARTITION_PATH).writeText(objectMapper.writeValueAsString(PartitionConfig(leaders, replications)))
            updateSlave()
            status = ClusterStatus.GREEN
            reloadBrokerConfigs()
        }
    }

    private fun doBrokersConfigs() {
        Thread {
            while (true) {
                val data = brokerRegistryQueue.poll()
                if (data != null)
                    registerBroker(data.host, data.port)
                Thread.sleep(10000)
            }
        }.start()
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
        val outOfSyncFile = File(ZOOKEEPER_OUT_OF_SYNC_BROKER_PATH)
        if (outOfSyncFile.exists()) {
            if (file.length() != 0L)
                outOfSyncBrokers.addAll(objectMapper.readValue(outOfSyncFile.readText(), AllBrokers::class.java).brokers)
        } else {
            outOfSyncFile.parentFile.mkdirs()
            outOfSyncFile.createNewFile()
            outOfSyncFile.writeText(objectMapper.writeValueAsString(AllBrokers()))
        }
    }

    private fun doPartitionConfigs() {
        val partitionsTemp = mutableMapOf<Int, PartitionData>()
        val partitionFile = File(ZOOKEEPER_PARTITION_PATH)
        val configs = partitionFile
            .takeIf { it.exists() }
            ?.let { objectMapper.readValue(it.readText(), PartitionConfig::class.java) }
        val brokerCount = minimumBrokerCount
        val partitionCount = configs?.leaderPartitionList?.values?.flatten()?.toSet()?.size ?: (brokerCount * partitionPerBroker)
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
        logger.debug { "loaded partitions are: $partitions" }
        if (configs != null) {
            leaders = configs.leaderPartitionList
            replications = configs.replicaPartitionList
        } else {
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
            partitionFile.parentFile.mkdirs()
            partitionFile.createNewFile()
            partitionFile.writeText(text)
        }
        logger.debug { "partitions loaded are: $partitions" }
    }

    @Timed("zookeeper.update.slave")
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
                        partitions,
                        AllBrokers(outOfSyncBrokers),
                    ),
                    String::class.java
                ).body
            } catch (e: ResourceAccessException) {
                logger.warn { "Slave is dead" }
            }
        }
        updateGauges()
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
        logger.debug { "loaded consumers: $consumers" }
    }


    fun getConfigs(): AllConfigs {
        return AllConfigs(
            1,
            partitions.size,
            brokers,
            status,
        )
    }

    fun addToRegistryQueue(host: String, port: Int): RegisterDto {
        brokers.find { it.host == host && it.port == port }?.let {
            return RegisterDto(it.brokerId!!, false, getConfigs())
        }
        outOfSyncBrokers.find { it.host == host && it.port == port }?.let {
            outOfSyncBrokers.remove(it)
            File(ZOOKEEPER_OUT_OF_SYNC_BROKER_PATH).writeText(objectMapper.writeValueAsString(AllBrokers(outOfSyncBrokers)))
            return RegisterDto(null, true, getConfigs())
        }
        if (brokerRegistryQueue.none { it.host == host && it.port == port })
            brokerRegistryQueue.add(BrokerRegistryDto(host, port, Date()))
        return RegisterDto(null)
    }

    @Timed("zookeeper.register.broker")
    @Synchronized
    fun registerBroker(host: String, port: Int) {
        brokers.find { it.host == host && it.port == port }?.let { return }
        val id = (brokers.lastOrNull()?.brokerId ?: -1) + 1

        status = ClusterStatus.MISSING_BROKERS
        reloadBrokerConfigs()

        if (id > minimumBrokerCount - 1) {
            val pId = partitions.values.maxOf { it.id!! } + 1
            val range = pId  until pId + partitionPerBroker
            partitions += range.associateWith { PartitionData(it, -1, -1, Date()) }
            partitionFileQueues += range.associateWith { PriorityBlockingQueue<PartitionData>() }
            range.forEach { partition ->
                val file = File(getPartitionPath(partition))
                if (file.exists()) throw Exception("partition $partition file already exists in Zookeeper!")
                file.parentFile.mkdirs()
                logger.debug { "Making new partition file $partition with data ${partitions[partition]}" }
                file.createNewFile()
                file.writeText(objectMapper.writeValueAsString(partitions[partition]!!))
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
                // no idea why this line should be commented
//                replications[broker.brokerId]!!.add(it)
                brokers.find { b -> b.brokerId == broker.brokerId }!!.config!!.replicaPartitionList.add(it)
                logger.debug { "added mapping replica of partition $it to ${broker.brokerId}: ${replications[broker.brokerId]!!}" }
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
    }

    @Timed("zookeeper.register.consumer")
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

    @Timed("zookeeper.unregister.consumer")
    fun unregisterConsumer(brokerId: Int, cId: Int) {
        if (status != ClusterStatus.GREEN) throw Exception("Cannot unregister consumer! cluster status is: ${status.name}")
        // notify other brokers
        status = ClusterStatus.REBALANCING
        reloadBrokerConfigs(brokerId)
        val newConsumers = consumers.filter { it.key != cId  }
        rebalanceConsumers(newConsumers)
        status = ClusterStatus.GREEN
        reloadBrokerConfigs(brokerId)
    }

    private fun rebalanceConsumers(newConsumers: Map<Int, MutableList<Int>> = consumers) {
        val cnt = newConsumers.keys.size
        if (cnt == 0) {
            consumers = mapOf()
            logger.warn { "No consumer present..." }
        }
        else {
            logger.debug { "Rebalancing partitions ${partitions.map { it.key }} on consumers ${newConsumers.keys}" }
            val availableConsumers = newConsumers.keys.toList()
            partitions.forEach { (id, data) ->
                newConsumers[availableConsumers[id % cnt]]!!.add(data.id!!)
            }
            consumers = newConsumers
        }
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
                            RegisterDto(it.brokerId, false, getConfigs()),
                            String::class.java
                        )
                        break
                    }
                    catch (e: ResourceAccessException) {
                        logger.error { "reloading failed for broker ${it.brokerId}. $e" }
                        Thread.sleep(1000)
                    }
                }
            }
    }

    @Timed("zookeeper.offset.update")
    fun updateLastOffset(id: Int, offset: Long) {
        logger.debug { "Updating last offset for partition: $id" }
        logger.debug { "Before update: $partitions" }
        val data = partitions[id]!!
        Counter.builder("message_count")
            .tags("pId", id.toString())
            .register(meterRegistry)
            .increment()
        data.lastOffset = offset
        data.timestamp = Date()
        partitionFileQueues[id]!!.add(data)
        logger.debug { "After update: $partitions" }
    }

    @Timed("zookeeper.commit.update")
    fun updateCommitOffset(id: Int, offset: Long) {
        logger.debug { "Updating commit offset for partition: $id" }
        logger.debug { "Before update: $partitions" }
        val data = partitions[id]!!
        Counter.builder("pull_count")
            .tags("pId", id.toString())
            .register(meterRegistry)
            .increment()
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

    @Timed("zookeeper.config.update")
    fun updateConfig(body: ZookeeperConfig) {
        if (isMaster) return
        brokers = body.allBrokers.brokers.toMutableList()
        leaders = body.partitionConfig.leaderPartitionList
        replications = body.partitionConfig.replicaPartitionList
        consumers = body.consumersConfig.consumers
        status = body.status
        outOfSyncBrokers.addAll(body.outOfSyncBrokers.brokers.toMutableList())
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
        val outOfSyncFile = File(ZOOKEEPER_OUT_OF_SYNC_BROKER_PATH)
        outOfSyncFile.writeText(objectMapper.writeValueAsString(AllBrokers(outOfSyncBrokers)))
        updateGauges()
    }

    fun clearAll() {
        status = ClusterStatus.MISSING_BROKERS
        reloadBrokerConfigs()
        rebalanceConsumers(mapOf())
        partitions = partitions.mapValues { PartitionData(it.key, -1, -1, Date()) }
        partitions.forEach { (id, data) ->
            partitionFileQueues[id]!!.add(data)
        }
        brokers.forEach {
            while (true) {
                try {
                    logger.debug { "clearing: ${it.brokerId}" }
                    restTemplate.postForEntity(
                        "http://${it.host}:${it.port}/config/clear",
                        null,
                        String::class.java
                    )
                    break
                }
                catch (e: ResourceAccessException) {
                    logger.error { "clearing failed for broker ${it.brokerId}. Retrying..." }
                }
            }
        }
        partitionFileQueues.forEach { it.value.clear() }
        status = ClusterStatus.GREEN
        reloadBrokerConfigs()
        updateGauges()
    }

    fun updateGauges() {
        Gauge.builder("consumer_count") { consumers.size }
            .register(meterRegistry)
        Gauge.builder("brokers") { brokers.size }
            .register(meterRegistry)
        partitions.forEach { (id, data) ->
            Gauge.builder("lag_partition") { data.lastOffset!! - data.lastCommit!! }
                .tags("id", id.toString())
                .register(meterRegistry)
            Gauge.builder("total_lag") { data.lastOffset!! - data.lastCommit!! }
                .register(meterRegistry)
        }
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
    val outOfSyncBrokers: AllBrokers,
)

data class BrokerRegistryDto (
        val host: String,
        val port: Int,
        val date: Date
        ): Comparable<BrokerRegistryDto> {
    override fun compareTo(other: BrokerRegistryDto): Int = date.compareTo(other.date)
}