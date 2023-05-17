//fun main(args: Array<String>) {
//    println("Hello World!")
//
//    // Try adding program arguments via Run/Debug configuration.
//    // Learn more about running applications: https://www.jetbrains.com/help/idea/running-applications.html.
//    println("Program arguments: ${args.joinToString()}")
//}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import kotlin.random.Random

data class Metadata(val id: Int, val metadata_url: String, val metadata: String)

fun main(args: Array<String>) {
    // Set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // Make parameters available in the web interface
    val params = ParameterTool.fromArgs(args)
    env.config.globalJobParameters = params

    // Create a stream of fake data
    val fakeDataStream: DataStream<Metadata> = env.generateSequence(1, 100).map { id ->
        Metadata(id.toInt(), "http://example.com/$id", "Fake metadata $id")
    }

    // Define the PostgreSQL sink
    val sink = object : SinkFunction<Metadata> {
        private var connection: Connection? = null
        private var insertStatement: PreparedStatement? = null

        override fun invoke(value: Metadata) {
            if (connection == null) {
                Class.forName("org.postgresql.Driver")
                connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb", "moss", "moss")
                insertStatement = connection!!.prepareStatement("INSERT INTO my_schema.data (id, metadata_url, metadata) VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET metadata_url = EXCLUDED.metadata_url, metadata = EXCLUDED.metadata")
            }
            insertStatement!!.setInt(1, value.id)
            insertStatement!!.setString(2, value.metadata_url)
            insertStatement!!.setString(3, value.metadata)
            insertStatement!!.executeUpdate()
        }
    }

    // Add the sink to the stream
    fakeDataStream.addSink(sink)

    // Execute the job
    env.execute("Flink Kotlin PostgreSQL Demo")
}
