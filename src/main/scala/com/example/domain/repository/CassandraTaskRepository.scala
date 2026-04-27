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
    deleteStmt: PreparedStatement,
    insertStmt: PreparedStatement,
    claimStmt: PreparedStatement,
    releaseStmt: PreparedStatement
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
          deleteStmt
            .bind()
            .setLong(NextExecutionTime, task.nextExecutionTime)
            .setLong(Segment, task.segment)
            .setLong(JobId, task.jobId),
          insertStmt
            .bind()
            .setLong(NextExecutionTime, task.nextExecutionTime)
            .setLong(Segment, task.segment)
            .setBoolean(Processed, true)
            .setLong(JobId, task.jobId)
            .setInt(Priority, task.priority)
            .setLong(Recurrence, task.recurrence)
            .setLong(OriginalExecutionTime, task.originalExecutionTime),
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
      .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
      .build()
    session.timedExecuteAsync(batch).map(_ => ())
  }

  override def claimTask(task: Task, workerId: String): Future[Option[Task]] =
    session
      .timedExecuteAsync(
        claimStmt
          .bind()
          .setLong(CassandraTaskRepository.JobIdField, task.jobId)
          .setLong(NextExecutionTime, task.nextExecutionTime)
          .setLong(Segment, task.segment)
          .setString(CassandraTaskRepository.WorkerIdField, workerId)
      )
      .map(rs => if (rs.one().getBoolean("[applied]")) Some(task) else None)

  override def releaseTask(task: Task): Future[Unit] =
    session
      .timedExecuteAsync(
        releaseStmt
          .bind()
          .setLong(CassandraTaskRepository.JobIdField, task.jobId)
          .setLong(NextExecutionTime, task.nextExecutionTime)
          .setLong(Segment, task.segment)
      )
      .map(_ => ())
}

object CassandraTaskRepository {
  import com.example.domain.model.Task.Schema._

  private val table        = "task_schedule"
  private val inFlightTable = "task_in_flight"

  // Column names for task_in_flight (kept private to companion to avoid
  // clashing with Task.Schema.JobId which is already in scope via wildcard import)
  private val JobIdField    = "job_id"
  private val WorkerIdField = "worker_id"

  private val selectCql =
    s"""
       |SELECT *
       |FROM $table
       |WHERE
       |$NextExecutionTime = :$NextExecutionTime AND
       |$Segment IN :$Segment AND
       |$Processed = false
       |""".stripMargin

  private val deleteCql =
    s"""
       |DELETE FROM $table
       |WHERE
       |$NextExecutionTime = :$NextExecutionTime AND
       |$Segment = :$Segment AND
       |$Processed = false AND
       |$JobId = :$JobId
       |""".stripMargin

  private val insertCql =
    s"""
       |INSERT INTO $table
       |($NextExecutionTime, $Segment, $Processed, $JobId, $Priority, $Recurrence, $OriginalExecutionTime)
       |VALUES (:$NextExecutionTime, :$Segment, :$Processed, :$JobId, :$Priority, :$Recurrence, :$OriginalExecutionTime)
       |""".stripMargin

  private val claimCql =
    s"""
       |INSERT INTO $inFlightTable ($JobIdField, $NextExecutionTime, $Segment, $WorkerIdField)
       |VALUES (:$JobIdField, :$NextExecutionTime, :$Segment, :$WorkerIdField)
       |IF NOT EXISTS
       |""".stripMargin

  private val releaseCql =
    s"""
       |DELETE FROM $inFlightTable
       |WHERE $JobIdField = :$JobIdField
       |AND $NextExecutionTime = :$NextExecutionTime
       |AND $Segment = :$Segment
       |""".stripMargin

  def create(session: CqlSession)(implicit ec: ExecutionContext): Future[CassandraTaskRepository] =
    for {
      sel     <- session.prepareAsync(selectCql).toScala
      del     <- session.prepareAsync(deleteCql).toScala
      ins     <- session.prepareAsync(insertCql).toScala
      claim   <- session.prepareAsync(claimCql).toScala
      release <- session.prepareAsync(releaseCql).toScala
    } yield new CassandraTaskRepository(session, sel, del, ins, claim, release)
}
