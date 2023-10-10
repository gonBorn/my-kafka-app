package app

import app.schema._
import cats.effect.{ExitCode, IO, IOApp}
import io.circe._
import io.debezium.config.Configuration
import io.debezium.embedded.EmbeddedEngine

import java.time.LocalDate
import java.util.Properties

object Main extends IOApp {

  private val RELEASE_DATE = LocalDate.parse("2022-10-01")
  private val MY_TECH_BOOK = Book("Mark", "Intelligent Tech Book", Tech, 100, RELEASE_DATE)
  private val MY_COMIC_BOOK = Book("Elon", "Funny Comic Book", Comic, 100, RELEASE_DATE)
  private val MY_OTHER_BOOK = Book("Lala", "A Random Book", Other, 100, RELEASE_DATE)

  override def run(): IO[ExitCode] = {
    bootstrap
      .as(ExitCode.Success)
      .handleErrorWith(e => {
        e.printStackTrace()
        IO(ExitCode.Error)
      })
  }

  private def bootstrap: IO[Unit] = for {
    _ <- IO(println("starting producer ..."))
    _ <- startProducer().foreverM
  } yield ()

  private def parseArgs(args: List[String]): IO[BookType] = {
    if (args.length < 1)
      IO.raiseError(new IllegalArgumentException("Need Book Type: TECH or COMIC"))
    else
      args(0) match {
        case Tech.value => IO.pure(Tech)
        case Comic.value => IO.pure(Comic)
        case _ => IO.raiseError(new IllegalArgumentException("Invalid BookType"))
      }
  }

  private def selectBook(bookType: BookType): Book = {
    bookType match {
      case Tech => MY_TECH_BOOK
      case Comic => MY_COMIC_BOOK
      case _ => MY_OTHER_BOOK
    }
  }

  private def startProducer(): IO[Unit] = {
    val producerResource = producer.buildKafkaProducerResource

    val config = Configuration.from(generateProps())
    //    val mySqlConnectorConfig = new PostgresConnectorConfig(config)

    println("######## Started")

    producerResource.use {
      producerClient => {
        val engine = EmbeddedEngine.create()
          .using(config)
          .notifying(record => {
            // 将Debezium捕获的变化事件发送到Kafka
            println(s"######## record ${record}")
            val book = jawn.decode[Book](record.value().toString)
            book match {
              case Right(book) =>
                val key = book.title
                val value = producer.consCloudEvent(book)
                producer.send(producerClient, key, value)

              case Left(error) =>
                println(s"Error decoding JSON: $error")
            }
          })
          .build()

        IO(engine.run())
      }
    }
  }

  private def generateProps(): Properties = {
    val props = new Properties()
    props.put("database.hostname", "localhost")
    props.put("database.port", "5432")
    props.put("database.user", "postgres")
    props.put("database.password", "postgres")
    props.put("database.dbname", "postgres")
    props.put("database.useSSL", "false")
    props.put("include.schema.changes", "true")
    props.put("table.include.list", "public.books")
    props.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
    props
  }

}
