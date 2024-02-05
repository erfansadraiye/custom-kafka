package com.example.customkafka.modules.zookeeper

import com.example.customkafka.modules.common.AllConfigs
import mu.KotlinLogging
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

private val logger = KotlinLogging.logger {}

@RestController
@RequestMapping("/zookeeper")
class ZookeeperController(
    val zookeeperService: ZookeeperService,
) {

    @PostMapping("/register")
    fun register(
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


}

data class RegisterRequest(
    val host: String,
    val port: Int,
)