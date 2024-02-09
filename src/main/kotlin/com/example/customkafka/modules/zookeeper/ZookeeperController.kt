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
    ): ResponseEntity<String> {
        logger.info { "Registering broker with host: ${request.host} and port: ${request.port}" }
        val id = zookeeperService.registerBroker(request.host, request.port)
        return ResponseEntity.ok(id.toString())
    }

    @PostMapping("/config")
    fun getConfigs(): ResponseEntity<AllConfigs> {
        val config = zookeeperService.getConfigs()
        return ResponseEntity.ok(config)
    }


    @PostMapping("/consumer/register/{bId}")
    fun registerConsumer(@PathVariable bId: Int): ResponseEntity<String> {
        logger.info { "Registering consumer..." }
        val id = zookeeperService.registerConsumer(bId)
        logger.info { "Registered consumer with id $id" }
        return ResponseEntity.ok(id.toString())
    }

    @PostMapping("/offset/commit")
    fun updateCommitOffset(
        @RequestBody body: PartitionDto
    ): ResponseEntity<*> {
        zookeeperService.updateCommitOffset(body.partitionId!!, body.offset!!)
        return ResponseEntity.ok("Offset updated successfully")
    }

    @PostMapping("/offset/last")
    fun updateLastOffset(
        @RequestBody body: PartitionDto
    ): ResponseEntity<*> {
        zookeeperService.updateLastOffset(body.partitionId!!, body.offset!!)
        return ResponseEntity.ok("Offset updated successfully")
    }

    @PostMapping("/partition/{id}")
    fun getPartitionForConsumer(@PathVariable id: Int): ResponseEntity<*> {
        val data = zookeeperService.getPartitionOffsetForConsumer(id)
        val dto = PartitionDto(data?.id, data?.lastCommit)
        logger.debug { "Partition data: $dto" }
        return ResponseEntity.ok<PartitionDto>(dto)
    }

}

data class RegisterRequest(
    val host: String,
    val port: Int,
)