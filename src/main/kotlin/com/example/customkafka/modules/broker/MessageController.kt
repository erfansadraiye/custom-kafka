package com.example.customkafka.modules.broker

import com.example.customkafka.modules.common.PartitionData
import mu.KotlinLogging
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*


private val logger = KotlinLogging.logger {}

@RestController
@RequestMapping("/message")
class MessageController(
    val messageService: MessageService
) {

    @PostMapping("/consume/{id}")
    fun consumeMessage(@PathVariable id: Int): ResponseEntity<*> {
        return try {
            //TODO add some url for ack
            val message = messageService.consume(id)
            ResponseEntity.ok<Message>(message)
        } catch (e: Exception) {
            logger.error { "Error sending message: $e" }
            ResponseEntity.badRequest().body("Error sending message")
        }
    }

    @PostMapping("/produce")
    fun sendMessage(
        @RequestBody request: MessageRequest
    ): ResponseEntity<*> {
        return try {
            messageService.produce(request.key, request.message)
            ResponseEntity.ok("Message sent successfully!")
        } catch (e: Exception) {
            logger.error { "Error sending message: ${e.printStackTrace()}" }
            ResponseEntity.badRequest().body("Error sending message")
        }
    }

    @PostMapping("/replica")
    fun getReplicaMessages(
        @RequestBody message: Message
    ): ResponseEntity<*> {
        return try {
            messageService.getReplicaMessages(message)
            ResponseEntity.ok("Message sent to replica successfully!")
        } catch (e: Exception) {
            logger.error { "Error sending message to replica: $e" }
            ResponseEntity.badRequest().body("Error sending message to replica")
        }
    }

    @PostMapping("/register")
    fun register(): ResponseEntity<String> {
        return try {
            val id = messageService.register()
            ResponseEntity.ok(id.toString())
        } catch (e: Exception) {
            logger.error { "Error while registering: $e" }
            ResponseEntity.badRequest().body("Error while registering!")
        }
    }

    @PostMapping("/ping")
    fun heartBeat(): ResponseEntity<*> {
        return ResponseEntity.ok("")
    }

}

data class MessageRequest(
    val key: String,
    val message: String
)
