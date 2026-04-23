package com.example.domain.actors

import akka.actor.{Actor, ActorLogging, Props, Scheduler, Timers}
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.scaladsl.Source
import akka.stream.{ActorAttributes, Materializer, Supervision}
import com.dslplatform.json.DslJson
import com.example.domain.model.{ConsistentHashRing, Task}
import com.example.domain.repository.TaskRepository
import com.example.infrastructure.configuration.AppConfig
import org.apache.kafka.clients.producer.ProducerRecord

import java.time.{Duration, Instant}
import scala.concurrent.ExecutionContext.parasitic
import scala.concurrent.{ExecutionContext, Future}

class WorkerActor(
    val repository: TaskRepository,
    val producerSettings: ProducerSettings[Array[Byte], Array[Byte]],
    val hashRing: ConsistentHashRing[Long]
)(implicit
    val materializer: Materializer
) extends Actor
    with ActorLogging
    with Timers {
  import MasterActor._
  import WorkerActor._
  import com.example.domain.model.Task._

  implicit val ec: ExecutionContext.parasitic.type = parasitic
  implicit def scheduler: Scheduler = context.system.scheduler

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
      Supervision.Stop
  }

  override def preStart(): Unit = {
    timers.startTimerAtFixedRate(
      "task-polling-scheduler",
      TaskPollingScheduler,
      Duration.ZERO,
      Duration.ofSeconds(60)
    )
  }

  override def receive: Receive = handleTaskScheduling(Set.empty)

  private def handleTaskScheduling(segments: Set[String]): Receive = {
    case SegmentAssignment(segments) =>
      context.become(handleTaskScheduling(segments))

    case TaskPollingScheduler if segments.nonEmpty =>
      Source
        .future(repository.getTasksDueForExecution(segments))
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

    case TaskPollingScheduler if segments.isEmpty =>
      log.info("Not ready to start polling tasks yet ;(")

    case TaskSent(task) =>
      val nextOriginalExecutionTime = Instant
        .ofEpochSecond(task.originalExecutionTime)
        .plusSeconds(task.recurrence)
        .getEpochSecond
      val nextSegment = hashRing
        .getNode(task.originalExecutionTime.toString)
        .getOrElse(0L)
      repository.recordTaskSentAndScheduleNext(task, nextSegment, nextOriginalExecutionTime)
  }
}

object WorkerActor {
  case object TaskPollingScheduler
  case class TaskSent(task: Task)

  def props(
      repository: TaskRepository,
      producerSettings: ProducerSettings[Array[Byte], Array[Byte]],
      hashRing: ConsistentHashRing[Long]
  )(implicit
      materializer: Materializer
  ): Props = Props(new WorkerActor(repository, producerSettings, hashRing))
}
