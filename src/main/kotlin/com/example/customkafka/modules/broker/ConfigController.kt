package com.example.customkafka.modules.broker

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
    fun reloadConfig(): ResponseEntity<*> {
        configHandler.reload()
        fileHandler.assignPartition()
        return ResponseEntity.ok("Config reloaded!")
    }

    @PostMapping("/make-leader/{id}")
    fun makeLeader(@PathVariable id: Int): ResponseEntity<String> {
        fileHandler.makeLeader(id)
        return ResponseEntity.ok("leader switching completed successfully!")
    }


}