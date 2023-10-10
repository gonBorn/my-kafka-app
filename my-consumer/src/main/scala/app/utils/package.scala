package app

import io.cloudevents.CloudEvent
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords}

import java.util.Properties
import scala.jdk.CollectionConverters.IterableHasAsScala

package object utils {
  def initProps(): Properties = {
    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-topic-group")
    // 如果设置为 true，消费者会自动定期提交它所消费的分区的偏移量，以记录它所消费的消息的位置。如果设置为 false，消费者需要手动编写代码来提交偏移量
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    // 指定当消费者没有偏移量时的起始偏移量，可以是"earliest"（从最早的消息开始）或"latest"（从最新的消息开始）
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps
  }

  def printRecordsInfo(records: ConsumerRecords[String, CloudEvent]): Unit = {
    println(s"Just polled ${records.count()} records.")

    val recordList: List[ConsumerRecord[String, CloudEvent]] = records.asScala.toList

    recordList.foreach {
      r => {
        println("--------------------- record info ----------------------------")
        println(s"record key: ${r.key()}")
        println(s"record offset: ${r.offset()}")
        println(s"record topic: ${r.topic()}")
      }
    }
  }

  def printStringRecordsInfo(records: ConsumerRecords[String, String]): Unit = {
    println(s"Just polled ${records.count()} records.")

    val recordList: List[ConsumerRecord[String, String]] = records.asScala.toList

    recordList.foreach {
      r => {
        println("--------------------- string record info ----------------------------")
        println(s"string record key: ${r.key()}")
        println(s"string record offset: ${r.offset()}")
        println(s"string record topic: ${r.topic()}")
        println(s"string record value: ${r.value()}")
      }
    }
  }
}
