////fun main(args: Array<String>) {
////    println("Hello World!")
////
////    // Try adding program arguments via Run/Debug configuration.
////    // Learn more about running applications: https://www.jetbrains.com/help/idea/running-applications.html.
////    println("Program arguments: ${args.joinToString()}")
////}
//import org.apache.commons.lang3.ObjectUtils
//import org.apache.flink.api.common.functions.MapFunction
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.functions.sink.SinkFunction
//import java.sql.Connection
//import java.sql.DriverManager
//import java.sql.PreparedStatement
//import kotlinx.coroutines.Dispatchers
//import kotlinx.coroutines.delay
//import kotlinx.coroutines.runBlocking
//import kotlinx.coroutines.withContext
//import java.io.BufferedReader
//import java.net.HttpURLConnection
//import java.net.URL
//import kotlin.random.Random
//import java.io.IOException
//import java.net.URI
//import java.util.Base64
//import kotlinx.serialization.json.Json
//import kotlinx.serialization.json.JsonObject
//import kotlinx.serialization.json.JsonContentPolymorphicSerializer
//import kotlinx.serialization.json.jsonPrimitive
//import kotlin.system.measureNanoTime
//
//data class Metadata(val id: Int, val metadata_url: String, var metadata: String)
//
//fun main(args: Array<String>) {
//    // Set up the execution environment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment()
//
//    // Make parameters available in the web interface
//    val params = ParameterTool.fromArgs(args)
//    env.config.globalJobParameters = params
//
//    // Create a stream of fake data
////    val fakeDataStream: DataStream<Metadata> = env.generateSequence(1, 100).map { id ->
////        Metadata(id.toInt(), "http://example.com/$id", "Fake metadata $id")
////    }
//
//    val fakeDataStream: DataStream<Metadata> = env.fromElements(
//        Metadata(1, "ipfs://QmQGsXVt5o8Qf2J3to21RJYdHsNZQaVFosPYNSMS5CHW7U/2.json", ""),
//        Metadata(2, "https://arweave.net/KTpuWvFpa8Fgj7t1dUVwZ2yhpfHWMcN8vkziGrwxbcg", ""),
//        Metadata(3, "data:application/json;base64,eyJpZCI6IDEyM30K", ""),
//    )
//    fun modifyMetadata(metadata: Metadata): Metadata {
//        return metadata
//    }
//
//suspend fun httpGet(url: String, retries: Int = 3): String = withContext(Dispatchers.IO) {
//    var attempts = 0
//    while (attempts < retries) {
//        try {
//            val connection = URL(url).openConnection() as HttpURLConnection
//            try {
//                return@withContext connection.inputStream.bufferedReader().use(BufferedReader::readText)
//            } finally {
//                connection.disconnect()
//            }
//        } catch (e: IOException) {
//            attempts++
//            if (attempts == retries) {
//                throw e
//            }
//            println("============>")
//            delay(1000L * attempts) // Exponential back-off
//        }
//    }
//    throw IOException("Failed to make HTTP request after $retries attempts")
//}
////suspend fun httpGet(url: String): String = withContext(Dispatchers.IO) {
////    val connection = URL(url).openConnection() as HttpURLConnection
////    try {
////        connection.inputStream.bufferedReader().use(BufferedReader::readText)
////    } finally {
////        connection.disconnect()
////    }
////}
//
//    val httpHostUrls = listOf(
//        "https://cloudflare-ipfs.com/",//1
//        "https://cf-ipfs.com/",//1
//        "https://gateway.pinata.cloud/",//1
//        "https://4everland.io/",//1
//        "https://ipfs.yt/",//1
//        "https://gateway.ipfs.io/",//1
//        "https://ipfs.io/",//1
//    )
//    val random = Random.Default
//
//    val afterBase64 = fakeDataStream.map {metadata ->
//        if (metadata.metadata_url.startsWith("data:application/json;base64,")) {
//            var base64Data = metadata.metadata_url.substringAfter("data:application/json;base64,")
//            val decodedBytes = Base64.getDecoder().decode(base64Data)
//            val jsonString = String(decodedBytes, Charsets.UTF_8)
////            val json = Json.parseToJsonElement(jsonString) as JsonObject
//            metadata.metadata = jsonString
//        }
//        metadata
//    }
//
//    val afterIPFSStream: DataStream<Metadata> = afterBase64.map {metadata ->
//        val randomUrls = httpHostUrls.shuffled(random).take(2)
//        if (metadata.metadata_url.startsWith("ipfs")) {
//                val url = URI(randomUrls.get(0)).resolve("ipfs/").resolve(URI(metadata.metadata_url.substringAfter("ipfs://")))
//                println("===> $url")
//                val metadataContent = runBlocking {
//                    try {
//                        httpGet(url.toString())
//                    } catch (e: IOException) {
//                        try {
//                            val url = URI(randomUrls.get(1)).resolve("ipfs/").resolve(URI(metadata.metadata_url.substringAfter("ipfs://")))
//                            httpGet(url.toString())
//                        } catch (e: IOException) {
//                            e.toString()
//                        }
//                    }
//                }
//                Metadata(metadata.id, metadata.metadata_url, metadataContent)
//        } else {
//            metadata
//        }
//    }
//
//    val modifiedDataStream: DataStream<Metadata> = afterIPFSStream.map { metadata ->
//        if (metadata.metadata_url.startsWith("http")) {
//            val metadataContent = runBlocking {
//                try {
//                    httpGet(metadata.metadata_url)
//                } catch (e: IOException) {
//                    e.toString()
//                }
//            }
//            Metadata(metadata.id, metadata.metadata_url, metadataContent)
//        } else {
//            metadata
//        }
//    }
//
//
//    // Define the PostgreSQL sink
//    val sink = object : SinkFunction<Metadata> {
//        private var connection: Connection? = null
//        private var insertStatement: PreparedStatement? = null
//
//        override fun invoke(value: Metadata) {
//            if (connection == null) {
//                Class.forName("org.postgresql.Driver")
//                connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb", "moss", "moss")
//                insertStatement = connection!!.prepareStatement("INSERT INTO my_schema.data (id, metadata_url, metadata) VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET metadata_url = EXCLUDED.metadata_url, metadata = EXCLUDED.metadata")
//            }
//            insertStatement!!.setInt(1, value.id)
//            insertStatement!!.setString(2, value.metadata_url)
//            insertStatement!!.setString(3, value.metadata)
//            insertStatement!!.executeUpdate()
//        }
//    }
//
//    // Add the sink to the stream
////    fakeDataStream.addSink(sink)
//    modifiedDataStream.addSink(sink)
//    // Execute the job
//    env.execute("Flink Kotlin PostgreSQL Demo")
//}


import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.net.HttpURLConnection
import java.net.URI
import java.net.URL
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.io.BufferedReader
import java.io.IOException
import java.util.Base64
import kotlin.random.Random

data class Metadata(val id: Int, val metadata_url: String, var metadata: String)

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val params = ParameterTool.fromArgs(args)
    env.config.globalJobParameters = params

    val fakeDataStream: DataStream<Metadata> = env.fromElements(
        Metadata(1, "ipfs://QmQGsXVt5o8Qf2J3to21RJYdHsNZQaVFosPYNSMS5CHW7U/2.json", ""),
        Metadata(2, "https://arweave.net/KTpuWvFpa8Fgj7t1dUVwZ2yhpfHWMcN8vkziGrwxbcg", ""),
        Metadata(3, "data:application/json;base64,eyJpZCI6IDEyM30K", ""),
    )

    val httpHostUrls = listOf(
        "https://cloudflare-ipfs.com/",
        "https://cf-ipfs.com/",
        "https://gateway.pinata.cloud/",
        "https://4everland.io/",
        "https://ipfs.yt/",
        "https://gateway.ipfs.io/",
        "https://ipfs.io/",
    )
    val random = Random.Default

    val afterBase64 = fakeDataStream.map { decodeBase64(it) }
    val afterIPFSStream: DataStream<Metadata> = afterBase64.map { resolveIpfs(it, random, httpHostUrls) }
    val modifiedDataStream: DataStream<Metadata> = afterIPFSStream.map { resolveHttp(it) }
    modifiedDataStream.addSink(PostgresSink())
    env.execute("Flink Kotlin PostgreSQL Demo")
}

fun decodeBase64(metadata: Metadata): Metadata {
    if (metadata.metadata_url.startsWith("data:application/json;base64,")) {
        val base64Data = metadata.metadata_url.substringAfter("data:application/json;base64,")
        val decodedBytes = Base64.getDecoder().decode(base64Data)
        val jsonString = String(decodedBytes, Charsets.UTF_8)
        metadata.metadata = jsonString
    }
    return metadata
}

suspend fun httpGet(url: String, retries: Int = 3): String = withContext(Dispatchers.IO) {
    var attempts = 0
    while (attempts < retries) {
        try {
            val connection = URL(url).openConnection() as HttpURLConnection
            try {
                return@withContext connection.inputStream.bufferedReader().use(BufferedReader::readText)
            } finally {
                connection.disconnect()
            }
        } catch (e: IOException) {
            attempts++
            if (attempts == retries) throw e
            delay(1000L * attempts)
        }
    }
    throw IOException("Failed to make HTTP request after $retries attempts")
}

fun resolveIpfs(metadata: Metadata, random: Random, httpHostUrls: List<String>): Metadata {
    if (metadata.metadata_url.startsWith("ipfs")) {
        val randomUrls = httpHostUrls.shuffled(random).take(2)
        val url = URI(randomUrls[0]).resolve("ipfs/").resolve(URI(metadata.metadata_url.substringAfter("ipfs://")))
        val metadataContent = runBlocking {
            httpGetSafe(url.toString(), randomUrls[1])
        }
        return Metadata(metadata.id, metadata.metadata_url, metadataContent)
    }
    return metadata
}

suspend fun httpGetSafe(url: String, fallbackUrl: String): String {
    return try {
        httpGet(url)
    } catch (e: IOException) {
        httpGet(fallbackUrl)
    }
}

fun resolveHttp(metadata: Metadata): Metadata {
    if (metadata.metadata_url.startsWith("http")) {
        val metadataContent = runBlocking {
            httpGetSafe(metadata.metadata_url, metadata.metadata_url)
        }
        return Metadata(metadata.id, metadata.metadata_url, metadataContent)
    }
    return metadata
}

class PostgresSink : SinkFunction<Metadata> {
    private var connection: Connection? = null
    private var insertStatement: PreparedStatement? = null
    override fun invoke(value: Metadata) {
        try {
            if (connection == null) {
                Class.forName("org.postgresql.Driver")
                connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb", "moss", "moss")
                insertStatement =
                    connection!!.prepareStatement("INSERT INTO my_schema.data (id, metadata_url, metadata) VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET metadata_url = EXCLUDED.metadata_url, metadata = EXCLUDED.metadata")
            }
            insertStatement!!.setInt(1, value.id)
            insertStatement!!.setString(2, value.metadata_url)
            insertStatement!!.setString(3, value.metadata)
            insertStatement!!.executeUpdate()
        } finally {
            insertStatement?.close()
            connection?.close()
        }
    }
}