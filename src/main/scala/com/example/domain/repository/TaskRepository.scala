package com.example.domain.repository

import com.example.domain.model.Task

import scala.concurrent.Future

trait TaskRepository {
  def getTasksDueForExecution(segments: Set[String]): Future[List[Task]]
  def recordTaskSentAndScheduleNext(
      task: Task,
      nextSegment: Long,
      nextOriginalExecutionTime: Long
  ): Future[Unit]
}
