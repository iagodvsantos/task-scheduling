package com.example.domain.model

import com.dslplatform.json.DslJson

import java.io.ByteArrayOutputStream

case class Task(
    jobId: Long,
    nextExecutionTime: Long,
    originalExecutionTime: Long,
    segment: Long,
    priority: Int,
    processed: Boolean,
    recurrence: Long
)

object Task {
  implicit class TaskSerialization(task: Task) {
    def serialize(serializer: DslJson[Any]): Array[Byte] = {
      val outputStream = new ByteArrayOutputStream()
      serializer.serialize(task, outputStream)
      outputStream.toByteArray
    }

  }

  val TaskScheduleTable = "task_schedule"

  object Schema {
    val JobId = "job_id"
    val NextExecutionTime = "next_execution_time"
    val Segment = "segment"
    val Priority = "priority"
    val Processed = "processed"
    val Recurrence = "recurrence"
    val OriginalExecutionTime = "original_execution_time"
  }

}
