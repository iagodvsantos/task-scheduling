package com.example.domain.actors

import akka.actor.{Actor, ActorLogging, Props, Scheduler, Timers}
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.datastax.oss.driver.api.core.cql.{Row, SimpleStatement}
import com.example.utils.EnrichDatastax._

import java.time.{Duration, LocalDateTime, ZoneOffset}
import scala.concurrent.ExecutionContext.parasitic
import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, blocking}
import scala.language.implicitConversions
import scala.util.Try

class WorkerActor(val cqlSession: CqlSession)
    extends Actor
    with ActorLogging
    with Timers {
  import MasterActor._
  import WorkerActor._
  import WorkerActor.Schema._

  implicit val ec: ExecutionContext.parasitic.type = parasitic

  implicit def scheduler: Scheduler = context.system.scheduler

  implicit def fromRow(r: Row): Try[Task] = Try {
    Task(
      r.getLong(JobId),
      r.getLong(NextExecutionTime),
      r.getLong(Segment)
    )
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
    case TaskPollingScheduler if segments.empty =>
      log.info("Not ready to start polling tasks yet ;(")
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
           |$Segment IN :$Segment
       """.stripMargin
        )
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
        .setIdempotent(true)
    )
  )
}

object WorkerActor {
  case class Task(job_id: Long, nextExecutionTime: Long, segment: Long)
  case object TaskPollingScheduler

  val TaskScheduleTable = "task_schedule"
  object Schema {
    val JobId = "job_id"
    val NextExecutionTime = "next_execution_time"
    val Segment = "segment"
  }

  def props(cqlSession: CqlSession): Props = Props(
    new WorkerActor(cqlSession)
  )
}
