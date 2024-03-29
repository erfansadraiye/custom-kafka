package com.example.customkafka.modules.broker

import mu.KotlinLogging
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*


private val logger = KotlinLogging.logger {}

@RestController
@RequestMapping("/message")
class MessageController(
    val brokerService: BrokerService
) {

    @PostMapping("/consume/{id}")
    fun consumeMessage(@PathVariable id: Int): ResponseEntity<*> {
        return try {
            //TODO add some url for ack
            val message = brokerService.consume(id)
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
            brokerService.produce(request.key, request.message)
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
            brokerService.getReplicaMessages(message)
            ResponseEntity.ok("Message sent to replica successfully!")
        } catch (e: Exception) {
            logger.error { "Error sending message to replica: $e" }
            ResponseEntity.badRequest().body("Error sending message to replica")
        }
    }

    @PostMapping("/register")
    fun register(): ResponseEntity<String> {
        return try {
            val id = brokerService.register()
            ResponseEntity.ok(id.toString())
        } catch (e: Exception) {
            logger.error { "Error while registering: $e" }
            ResponseEntity.badRequest().body("Error while registering!")
        }
    }

    @PostMapping("/unregister/{cId}")
    fun unregister(@PathVariable cId: String): ResponseEntity<String> {
        return try {
            val id = brokerService.unregister(cId)
            ResponseEntity.ok(id.toString())
        } catch (e: Exception) {
            logger.error { "Error while unregistering: $e" }
            ResponseEntity.badRequest().body("Error while registering!")
        }
    }

    @PostMapping("/ack/{pId}/{offset}")
    fun ack(@PathVariable pId: Int, @PathVariable offset: Long): ResponseEntity<String> {
        brokerService.ack(pId, offset)
        return ResponseEntity.ok("ack completed successfully")
    }


    @PostMapping("/ping")
    fun heartBeat(): ResponseEntity<*> {
        return ResponseEntity.ok("ok")
    }

}

data class MessageRequest(
    val key: String,
    val message: String
)
