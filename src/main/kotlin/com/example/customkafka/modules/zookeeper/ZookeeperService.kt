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

private val logger = KotlinLogging.logger {}

@Service
class ZookeeperService(
    @Value("\${kafka.brokers.num}")
    val brokerCount: Int,
    @Value("\${kafka.partitions.num}")
    val partitionCount: Int,
    @Value("\${kafka.replication-factor}")
    val replicationFactor: Int,
    val restTemplate: RestTemplate,
) {
    //TODO need to load from config
    val config = ZookeeperConfig(true)

    var leaders = mapOf<Int, MutableList<Int>>()
    var replications = mapOf<Int, MutableList<Int>>()

    var brokers = mutableListOf<BrokerConfig>()

    var consumers = mapOf<Int, MutableList<Int>>()

    var status = ClusterStatus.MISSING_BROKERS

    var partitions = mapOf<Int, PartitionData>()

    @PostConstruct
    fun setup() {
        if (!config.isMaster) return
        doPartitionConfigs()
        doBrokersConfigs()
    }

    @Scheduled(fixedRateString = "\${kafka.heartbeat-interval}", initialDelayString = "60000")
    fun checkBrokerHealth() {
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
        deadBroker?.let {
            status = ClusterStatus.MISSING_BROKERS
            reloadBrokerConfigs()
            val deadLeaders = deadBroker.config!!.leaderPartitionList
            deadLeaders.forEach { leader ->
                val candidates = brokers.filter { it.config!!.replicaPartitionList.contains(leader) }
                //TODO do something else?
                if (candidates.isEmpty()) throw Exception("No Replica found for partition $leader. data is lost!")
                logger.debug { "found replica for leader $leader in broker ${candidates.map { it.brokerId }}" }
                candidates.random().config!!.apply {
                    replicaPartitionList.remove(leader)
                    leaderPartitionList.add(leader)
                    restTemplate.postForEntity(
                        "http://${it.host}:${it.port}/config/make-leader/$leader",
                        null,
                        String::class.java
                    )
                }
            }
            brokers.remove(deadBroker)
            status = ClusterStatus.GREEN
            reloadBrokerConfigs()
        }
    }

    private fun doBrokersConfigs() {
        val file = File("data/zookeeper/zookeeperBrokers.txt")
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
        val file = File("data/zookeeper/zookeeperPartitions.txt")
        if (file.exists()) {
            val configs = objectMapper.readValue(file.readText(), PartitionConfig::class.java)
            leaders = configs.leaderPartitionList
            replications = configs.replicaPartitionList
        } else {
            partitions = (0 until partitionCount).map { PartitionData(it, -1, -1) }.associateBy { it.id }
            leaders = (0 until brokerCount).associateWith { mutableListOf() }
            replications = (0 until brokerCount).associateWith { mutableListOf() }
            partitions.values.forEachIndexed { index, p ->
                leaders[index % brokerCount]!!.add(p.id)
            }
            partitions.values.forEachIndexed { index, p ->
                val availableBrokers = leaders.filter { !it.value.contains(p.id) }.map { it.key }
                if (availableBrokers.isEmpty()) throw Exception("Replication factor cannot be bigger than broker count!")
                repeat(replicationFactor) {
                    replications[availableBrokers[it]]!!.add(p.id)
                }
            }
            val text = objectMapper.writeValueAsString(PartitionConfig(leaders, replications))
            //TODO inform slave zookeeper about new file
            file.parentFile.mkdirs()
            file.createNewFile()
            file.writeText(text)
        }
    }


    fun getConfigs(): AllConfigs {
        return AllConfigs(
            replicationFactor,
            partitionCount,
            brokers,
            status,
        )
    }

    fun registerBroker(host: String, port: Int): Int {
        brokers.find { it.host == host && it.port == port }?.let {
            return it.brokerId!!
        }
        val id = (brokers.lastOrNull()?.brokerId ?: -1) + 1
        val config = BrokerConfig(id, host, port, MyConfig(leaders[id]!!, replications[id]!!))
        if (brokers.size + 1 == brokerCount) status = ClusterStatus.GREEN
        reloadBrokerConfigs()
        brokers.add(config)
        val file = File("data/zookeeper/zookeeperBrokers.txt")
        file.writeText(objectMapper.writeValueAsString(AllBrokers(brokers)))
        return id
    }

    fun registerConsumer(): Int {
        if (status != ClusterStatus.GREEN) throw Exception("Cannot register consumer! cluster status is: ${status.name}")
        // notify all brokers
        status = ClusterStatus.REBALANCING
        reloadBrokerConfigs()
        //TODO maybe wait for some time?
        val id = (consumers.keys.lastOrNull() ?: -1) + 1
        val newConsumers = consumers.mapValues { mutableListOf<Int>() } + mapOf(id to mutableListOf())
        val cnt = newConsumers.keys.size
        partitions.keys.forEach {
            newConsumers[it % cnt]!!.add(partitions[it]!!.id)
        }
        consumers = newConsumers
        logger.debug { "Consumers: $consumers" }
        status = ClusterStatus.GREEN
        reloadBrokerConfigs()
        return id
    }

    private fun reloadBrokerConfigs() {
        brokers.forEach {
            restTemplate.postForEntity(
                "http://${it.host}:${it.port}/config/reload",
                null,
                String::class.java
            )
        }
    }

    fun updateLastOffset(partition: PartitionData) {
        logger.debug { "Updating last offset for partition: $partition" }
        logger.debug { "Before update: $partitions" }
        partitions[partition.id]!!.lastOffset = partition.lastOffset
        logger.debug { "After update: $partitions" }
        //TODO write to file
    }

    fun updateCommitOffset(partition: PartitionData) {
        logger.debug { "Updating commit offset for partition: $partition" }
        logger.debug { "Before update: $partitions" }
        partitions[partition.id]!!.lastCommit = partition.lastCommit
        logger.debug { "After update: $partitions" }

        //TODO write to file
    }

    fun writeOffsets(partition: PartitionData) {

    }

    fun getDirectory(partition: PartitionData) {

    }

    fun getPartitionOffsetForConsumer(id: Int): PartitionData {
        val assignedPartitions = consumers[id] ?: throw Exception("un registered consumer id")
        val partition = assignedPartitions
            .map { partitions[it]!! }
            .maxBy { it.lastOffset - it.lastCommit }
        return partition
    }
}

data class AllBrokers(
    val brokers: List<BrokerConfig> = listOf()
)