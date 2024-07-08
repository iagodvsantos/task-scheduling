package com.example

import akka.actor.{ActorRef, ActorSystem, Props}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader
import com.example.domain.actors.{MasterActor, WorkerActor}
import com.typesafe.config.ConfigFactory

object Main extends App {

  def createNode(port: Int, role: String, props: Props, actorName: String): ActorRef = {
    val config = ConfigFactory.parseString(
      s"""
         |akka.cluster.roles = ["$role"]
         |akka.remote.artery.canonical.port = $port
         |""".stripMargin
    ).withFallback(ConfigFactory.load())

    val system = ActorSystem("TaskSchedulerCluster", config)
    system.actorOf(props, actorName)
  }

  val cqlSession = CqlSession.builder()
    .withKeyspace("task_scheduling")
    .build()

  createNode(2551, "master", Props[MasterActor], "master")
  createNode(2552, "worker", WorkerActor.props(cqlSession), "worker")
  createNode(2553, "worker", WorkerActor.props(cqlSession), "worker")

}
