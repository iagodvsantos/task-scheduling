package com.example.domain.repository

import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.datastax.oss.driver.api.core.cql._
import com.example.domain.model.Task
import com.example.domain.model.Task.Schema._
import com.example.infrastructure.utils.EnrichDatastax._

import java.time.Instant
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Try

class CassandraTaskRepository private (
    session: CqlSession,
    selectStmt: PreparedStatement,
    updateStmt: PreparedStatement,
    insertStmt: PreparedStatement
)(implicit ec: ExecutionContext)
    extends TaskRepository {

  private implicit def fromRow(r: Row): Try[Task] = Try {
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

  override def getTasksDueForExecution(segments: Set[String]): Future[List[Task]] = {
    val epochMinute = Instant.now().getEpochSecond / 60
    session
      .timedExecuteAsync(
        selectStmt
          .bind()
          .setLong(NextExecutionTime, epochMinute)
          .setSet(Segment, segments.asJava, classOf[String])
          .setBoolean(Processed, false)
      )
      .flatMap(_.fetchAll)
  }

  override def recordTaskSentAndScheduleNext(
      task: Task,
      nextSegment: Long,
      nextOriginalExecutionTime: Long
  ): Future[Unit] = {
    val batch = BatchStatement
      .builder(DefaultBatchType.LOGGED)
      .addStatements(
        List[BatchableStatement[_]](
          updateStmt
            .bind()
            .setLong(NextExecutionTime, task.nextExecutionTime)
            .setLong(Segment, task.segment)
            .setLong(JobId, task.jobId),
          insertStmt
            .bind()
            .setLong(NextExecutionTime, nextOriginalExecutionTime / 60)
            .setLong(Segment, nextSegment)
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
    session.timedExecuteAsync(batch).map(_ => ())
  }
}

object CassandraTaskRepository {
  import com.example.domain.model.Task.Schema._

  private val table = "task_schedule"

  private val selectCql =
    s"""
       |SELECT *
       |FROM $table
       |WHERE
       |$NextExecutionTime = :$NextExecutionTime AND
       |$Segment IN :$Segment AND
       |$Processed = :$Processed
       |""".stripMargin

  private val updateCql =
    s"""
       |UPDATE $table
       |SET $Processed = true
       |WHERE
       |$NextExecutionTime = :$NextExecutionTime AND
       |$Segment = :$Segment AND
       |$Processed = False AND
       |$JobId = :$JobId
       |""".stripMargin

  private val insertCql =
    s"""
       |INSERT INTO $table
       |($NextExecutionTime, $Segment, $Processed, $JobId, $Priority, $Recurrence, $OriginalExecutionTime)
       |VALUES (:$NextExecutionTime, :$Segment, :$Processed, :$JobId, :$Priority, :$Recurrence, :$OriginalExecutionTime)
       |""".stripMargin

  def create(session: CqlSession)(implicit ec: ExecutionContext): Future[CassandraTaskRepository] =
    for {
      sel <- session.prepareAsync(selectCql).toScala
      upd <- session.prepareAsync(updateCql).toScala
      ins <- session.prepareAsync(insertCql).toScala
    } yield new CassandraTaskRepository(session, sel, upd, ins)
}
