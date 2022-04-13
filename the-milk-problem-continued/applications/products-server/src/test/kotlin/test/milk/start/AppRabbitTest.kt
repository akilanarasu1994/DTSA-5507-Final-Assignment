package test.milk.start

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import io.ktor.http.*
import io.ktor.server.testing.*
import io.milk.products.PurchaseInfo
import io.milk.rabbitmq.BasicRabbitConfiguration
import io.milk.rabbitmq.RabbitTestSupport
import io.milk.start.module
import io.milk.testsupport.testDbPassword
import io.milk.testsupport.testDbUsername
import io.milk.testsupport.testJdbcUrl
import io.mockk.clearAllMocks
import org.junit.Before
import org.junit.Test
import test.milk.TestScenarioSupport
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AppRabbitTest {
    private val testSupport = RabbitTestSupport()
    private val engine = TestApplicationEngine()
    private val mapper: ObjectMapper = ObjectMapper().registerKotlinModule()

    @Before
    fun before() {
        BasicRabbitConfiguration("products-exchange", "products").setUp()
        testSupport.purge("products")

        BasicRabbitConfiguration("safer-products-exchange", "safer-products").setUp()
        testSupport.purge("safer-products")

        clearAllMocks()
        TestScenarioSupport().loadTestScenario("products")
        engine.start(wait = false)
        engine.application.module(testJdbcUrl, testDbUsername, testDbPassword)
    }

    @Test
    fun testQuantity_1() {
        makePurchase(PurchaseInfo(105442, "milk", 1), "products")
        testSupport.waitForConsumers("products")

        with(engine) {
            with(handleRequest(io.ktor.http.HttpMethod.Get, "/")) {
                assertTrue(response.content!!.contains("130"))
            }
        }
    }

    @Test
    fun testQuantity_50() {
        makePurchases(PurchaseInfo(105442, "milk", 1), "products")
        testSupport.waitForConsumers("products")

        with(engine) {
            with(handleRequest(io.ktor.http.HttpMethod.Get, "/")) {
                assertTrue(response.content!!.contains("81"))
            }
        }
    }

    @Test
    fun testSaferQuantity() {
        // TODO1 -
        //  test a "safer" purchase, one where you are using a different "safer" queue
        //  then wait for consumers,
        //  then make a request
        //  and assert that the milk count 130
        makePurchase(PurchaseInfo(105442, "milk", 1), "safer-products")
        testSupport.waitForConsumers("safer-products")

        with(engine) {
            with(handleRequest(io.ktor.http.HttpMethod.Get, "/")) {
                assertTrue(response.content!!.contains("130"))
            }
        }
    }

    @OptIn(ExperimentalStdlibApi::class)
    @Test
    fun testBestCase() {
        makePurchases(PurchaseInfo(105443, "bacon", 1), "safer-products")
        // TODO1 -
        //  uncomment the below after introducing the safer product update handler with manual acknowledgement
        testSupport.waitForConsumers("safer-products")

        with(engine) {
            with(handleRequest(HttpMethod.Get, "/")) {
                val compact = response.content!!.replace("\\s".toRegex(), "")
                val bacon = "<td>bacon</td><td>([0-9]+)</td>".toRegex().find(compact)!!.groups[1]!!.value
                assertTrue(bacon.toInt() < 72)
            }
        }
    }

    ///

    private fun makePurchase(purchase: PurchaseInfo, queue: String) {
        val factory = ConnectionFactory().apply { useNio() }
        factory.newConnection().use { connection ->
            connection.createChannel().use { channel ->
                val body = mapper.writeValueAsString(purchase).toByteArray()
                channel.basicPublish("", queue, MessageProperties.BASIC, body)
            }
        }
    }

    private fun makePurchases(purchase: PurchaseInfo, queue: String) {
        val factory = ConnectionFactory().apply { useNio() }
        factory.newConnection().use { connection ->
            connection.createChannel().use { channel ->
                (1..50).map {
                    val body = mapper.writeValueAsString(purchase).toByteArray()
                    channel.basicPublish("", queue, MessageProperties.PERSISTENT_BASIC, body)
                }
            }
        }
    }
}
