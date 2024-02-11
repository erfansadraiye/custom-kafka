package com.example.customkafka.modules.broker

import com.example.customkafka.modules.common.RegisterDto
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/config")
class ConfigController(
    val configHandler: ConfigHandler,
    val fileHandler: FileHandler,
) {

    @PostMapping("/reload")
    fun reloadConfig(
        registerDto: RegisterDto,
    ): ResponseEntity<*> {
        configHandler.reload(registerDto)
        fileHandler.assignPartition()
        return ResponseEntity.ok("Config reloaded!")
    }

    @PostMapping("/make-leader/{id}")
    fun makeLeader(@PathVariable id: Int): ResponseEntity<String> {
        fileHandler.makeLeader(id)
        return ResponseEntity.ok("leader switching completed successfully!")
    }

    @PostMapping("/clear")
    fun clear(): ResponseEntity<String> {
        fileHandler.clear()
        return ResponseEntity.ok("all messages deleted successfully!")
    }


}