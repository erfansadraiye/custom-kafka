package com.example.customkafka.modules.zookeeper

import com.example.customkafka.modules.common.AllConfigs
import com.example.customkafka.modules.common.PartitionDto
import mu.KotlinLogging
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

private val logger = KotlinLogging.logger {}

@RestController
@RequestMapping("/zookeeper")
class ZookeeperController(
    val zookeeperService: ZookeeperService,
) {

    @PostMapping("/broker/register")
    fun registerBroker(
        @RequestBody request: RegisterRequest,
    ): ResponseEntity<*> {
        zookeeperService.isHealthChecking = true
        logger.info { "Registering broker with host: ${request.host} and port: ${request.port}" }
        val dto = zookeeperService.addToRegistryQueue(request.host, request.port)
        if (dto.id != null || dto.clearDirectory) zookeeperService.updateSlave()
        return ResponseEntity.ok(dto)
    }

    @PostMapping("/config")
    fun getConfigs(): ResponseEntity<AllConfigs> {
        zookeeperService.isHealthChecking = true
        val config = zookeeperService.getConfigs()
        return ResponseEntity.ok(config)
    }


    @PostMapping("/consumer/register/{bId}")
    fun registerConsumer(@PathVariable bId: Int): ResponseEntity<String> {
        logger.info { "Registering consumer..." }
        zookeeperService.isHealthChecking = true
        val id = zookeeperService.registerConsumer(bId)
        zookeeperService.updateSlave()
        logger.info { "Registered consumer with id $id" }
        return ResponseEntity.ok(id.toString())
    }


    @PostMapping("/consumer/unregister/{bId}/{cId}")
    fun unregisterConsumer(@PathVariable bId: Int, @PathVariable cId: Int): ResponseEntity<String> {
        logger.info { "Unregistering consumer $cId..." }
        zookeeperService.isHealthChecking = true
        zookeeperService.unregisterConsumer(bId, cId)
        zookeeperService.updateSlave()
        logger.info { "Unregistered consumer with id $cId" }
        return ResponseEntity.ok("unregistered successfully")
    }

    @PostMapping("/offset/commit")
    fun updateCommitOffset(
        @RequestBody body: PartitionDto
    ): ResponseEntity<*> {
        zookeeperService.isHealthChecking = true
        zookeeperService.updateCommitOffset(body.partitionId!!, body.offset!!)
        zookeeperService.updateSlave()
        return ResponseEntity.ok("Offset updated successfully")
    }

    @PostMapping("/offset/last")
    fun updateLastOffset(
        @RequestBody body: PartitionDto
    ): ResponseEntity<*> {
        zookeeperService.isHealthChecking = true
        zookeeperService.updateLastOffset(body.partitionId!!, body.offset!!)
        zookeeperService.updateSlave()
        return ResponseEntity.ok("Offset updated successfully")
    }

    @PostMapping("/partition/{id}")
    fun getPartitionForConsumer(@PathVariable id: Int): ResponseEntity<*> {
        zookeeperService.isHealthChecking = true
        val data = zookeeperService.getPartitionOffsetForConsumer(id)
        val dto = PartitionDto(data?.id, data?.lastCommit)
        logger.debug { "Partition data: $dto" }
        return ResponseEntity.ok<PartitionDto>(dto)
    }

    @PostMapping("/update")
    fun updateConfig(
        @RequestBody body: ZookeeperConfig
    ) {
        zookeeperService.updateConfig(body)
    }

    @PostMapping("/health")
    fun health(): ResponseEntity<String> {
        return ResponseEntity.ok("ok")
    }

    @PostMapping("/clear")
    fun clear(): ResponseEntity<String> {
        zookeeperService.clearAll()
        zookeeperService.updateSlave()
        return ResponseEntity.ok("cleared")
    }
}

data class RegisterRequest(
    val host: String,
    val port: Int,
)