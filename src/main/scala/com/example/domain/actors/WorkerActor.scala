package com.example.domain.actors

import akka.actor.{Actor, ActorLogging, Props, Scheduler, Timers}
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.scaladsl.Source
import akka.stream.{ActorAttributes, Materializer, Supervision}
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.dslplatform.json.DslJson
import com.example.domain.model.{ConsistentHashRing, Task}
import com.example.infrastructure.configuration.AppConfig
import com.example.infrastructure.utils.EnrichDatastax._
import org.apache.kafka.clients.producer.ProducerRecord

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}
import scala.concurrent.ExecutionContext.parasitic
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.Try

class WorkerActor(
    val cqlSession: CqlSession,
    val producerSettings: ProducerSettings[Array[Byte], Array[Byte]],
    val hashRing: ConsistentHashRing[Long]
)(implicit
    val materializer: Materializer
) extends Actor
    with ActorLogging
    with Timers {
  import MasterActor._
  import WorkerActor._
  import com.example.domain.model.Task.Schema._
  import com.example.domain.model.Task._

  implicit val ec: ExecutionContext.parasitic.type = parasitic
  implicit def scheduler: Scheduler = context.system.scheduler

  implicit def fromRow(r: Row): Try[Task] = Try {
    Task(
      r.getLong(JobId),
      r.getLong(NextExecutionTime),
      r.getLong(OriginalExecutionTime),
      r.getLong(Segment),
      r.getInt(Priority),
      r.getBoolean(Processed),
      r.getLong(Recurrence)
    )
  }

  private val routingTopic =
    AppConfig.getString("task.scheduler.kafka-topics.execution")

  private val dslJson = new DslJson[Any](
    new DslJson.Settings()
      .includeServiceLoader()
  )

  private val streamSupervisor: Supervision.Decider = {
    case exception: Exception =>
      log.error("Failed sending message to kafka with exception: {}", exception)
      Supervision.Resume
    case _ =>
      Supervision.Stop // Stop for any other unexpected failures
  }

  override def preStart(): Unit = {
    val fixedIntervalRate =
      Duration.ofSeconds(60)
    timers.startTimerAtFixedRate(
      "task-polling-scheduler",
      TaskPollingScheduler,
      Duration.ZERO,
      fixedIntervalRate
    )
  }

  override def receive: Receive = handleTaskScheduling(Set.empty)

  private def handleTaskScheduling(segments: Set[String]): Receive = {
    case SegmentAssignment(segments) =>
      context.become(handleTaskScheduling(segments))
    case TaskPollingScheduler if segments.nonEmpty =>
      Source
        .future(
          cqlSession
            .timedExecuteAsync(
              getCurrentTasksForExecution
                .bind()
                .setLong(
                  NextExecutionTime,
                  LocalDateTime
                    .now()
                    .atZone(ZoneOffset.UTC)
                    .toInstant
                    .getEpochSecond / 60
                )
                .setSet(Segment, segments.asJava, classOf[String])
            )
            .flatMap(_.fetchAll)
        )
        .flatMapConcat(Source(_))
        .mapAsync(parallelism = 8) { task =>
          Future(
            ProducerMessage.single(
              new ProducerRecord[Array[Byte], Array[Byte]](
                routingTopic.format(task.priority),
                task.serialize(dslJson)
              ),
              task
            )
          )
        }
        .via(Producer.flexiFlow(producerSettings))
        .withAttributes(ActorAttributes.supervisionStrategy(streamSupervisor))
        .map {
          case ProducerMessage.Result(_, message) =>
            self ! TaskSent(message.passThrough)
          case _ =>
            log.warning("Unprocessable message type ;(")
        }
        .run()

    case TaskPollingScheduler if segments.empty =>
      log.info("Not ready to start polling tasks yet ;(")

    case TaskSent(task) =>
      val nextOriginalExecutionTime = Instant
        .ofEpochSecond(task.originalExecutionTime)
        .plusSeconds(task.recurrence)
        .getEpochSecond

      BatchStatement
        .builder(DefaultBatchType.LOGGED)
        .addStatements(
          List[BatchableStatement[_]](
            updateTaskStatus
              .bind()
              .setLong(NextExecutionTime, task.nextExecutionTime)
              .setLong(Segment, task.segment)
              .setLong(JobId, task.jobId),
            insertNextRecurrentTask
              .bind()
              .setLong(
                NextExecutionTime,
                nextOriginalExecutionTime / 60
              )
              .setLong(
                Segment,
                hashRing
                  .getNode(task.originalExecutionTime.toString)
                  .getOrElse(0)
              )
              .setBoolean(Processed, false)
              .setLong(JobId, task.jobId)
              .setInt(Priority, task.priority)
              .setLong(Recurrence, task.recurrence)
              .setLong(OriginalExecutionTime, nextOriginalExecutionTime)
          ).asJava
        )
        .setIdempotence(true)
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
        .build()

  }

  private[this] val getCurrentTasksForExecution = blocking(
    cqlSession.prepare(
      SimpleStatement
        .newInstance(
          s"""
           |SELECT *
           |FROM $TaskScheduleTable
           |WHERE
           |$NextExecutionTime = :$NextExecutionTime AND
           |$Segment IN :$Segment AND
           |$Processed = :$Processed
       """.stripMargin
        )
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
        .setIdempotent(true)
    )
  )

  private[this] val updateTaskStatus = blocking(
    cqlSession.prepare(
      SimpleStatement
        .newInstance(s"""
           |UPDATE
           |$TaskScheduleTable
           |SET
           |$Processed = True AND
           |WHERE
           |$NextExecutionTime = :$NextExecutionTime AND
           |$Segment = :$Segment AND
           |$Processed = False AND
           |$JobId = :$JobId
           |""".stripMargin)
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
        .setIdempotent(true)
    )
  )

  private[this] val insertNextRecurrentTask = blocking(
    cqlSession.prepare(
      SimpleStatement.newInstance(s"""
           |INSERT INTO $TaskScheduleTable
           |($NextExecutionTime, $Segment, $Processed, $JobId, $Priority, $Recurrence, $OriginalExecutionTime)
           |VALUES (:$NextExecutionTime, :$Segment, :$Processed, :$JobId, :$Priority, :$Recurrence, $OriginalExecutionTime)
           |""".stripMargin)
    )
  )
}

object WorkerActor {
  case object TaskPollingScheduler
  case class TaskSent(task: Task)

  def props(
      cqlSession: CqlSession,
      producerSettings: ProducerSettings[Array[Byte], Array[Byte]],
      hashRing: ConsistentHashRing[Long]
  )(implicit
      materializer: Materializer
  ): Props = Props(
    new WorkerActor(cqlSession, producerSettings, hashRing)
  )
}
