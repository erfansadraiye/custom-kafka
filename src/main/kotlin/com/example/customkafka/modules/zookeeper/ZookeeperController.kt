package com.example.customkafka.modules.zookeeper

import com.example.customkafka.modules.common.AllConfigs
import org.springframework.http.HttpHeaders
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*


@RestController
@RequestMapping("/zookeeper")
class ZookeeperController(
    val zookeeperService: ZookeeperService,
) {

    @PostMapping("/register")
    fun register(
        @RequestHeader(HttpHeaders.HOST) host: String,
        @RequestHeader("port") port: Int,
    ): ResponseEntity<String> {
        val id = zookeeperService.registerBroker(host, port)
        return ResponseEntity.ok(id.toString())
    }

    @PostMapping("/config")
    fun getConfigs(): ResponseEntity<AllConfigs> {
        val config = zookeeperService.getConfigs()
        return ResponseEntity.ok(config)
    }


}