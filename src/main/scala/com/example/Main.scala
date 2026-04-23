package com.example

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.kafka.ProducerSettings
import akka.stream.Materializer
import com.datastax.oss.driver.api.core.CqlSession
import com.example.domain.actors.{MasterActor, WorkerActor}
import com.example.domain.model.ConsistentHashRing
import com.example.domain.repository.CassandraTaskRepository
import com.example.infrastructure.configuration.AppConfig
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {

  def createNode(port: Int, role: String, propsOf: ActorSystem => Props, actorName: String): ActorRef = {
    val config = ConfigFactory.parseString(
      s"""
         |akka.cluster.roles = ["$role"]
         |akka.remote.artery.canonical.port = $port
         |""".stripMargin
    ).withFallback(ConfigFactory.load())

    val system = ActorSystem("TaskSchedulerCluster", config)
    system.actorOf(propsOf(system), actorName)
  }

  val cqlSession = CqlSession.builder()
    .withKeyspace("task_scheduling")
    .build()

  val producerSettings: ProducerSettings[Array[Byte], Array[Byte]] =
    ProducerSettings(AppConfig.getConfig("akka.kafka"), new ByteArraySerializer, new ByteArraySerializer)

  val hashRing = ConsistentHashRing[Long](
    replicas = AppConfig.getInt("task.scheduler.hash.ring.segments")
  )

  CassandraTaskRepository.create(cqlSession).foreach { repository =>
    createNode(2551, "master", _ => Props[MasterActor], "master")
    createNode(2552, "worker", sys => WorkerActor.props(repository, producerSettings, hashRing)(Materializer(sys)), "worker")
    createNode(2553, "worker", sys => WorkerActor.props(repository, producerSettings, hashRing)(Materializer(sys)), "worker")
  }
}
