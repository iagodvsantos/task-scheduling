package com.example.domain.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Address}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.pattern.pipe
import akka.util.Timeout
import com.example.domain.model.ConsistentHashRing

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class MasterActor extends Actor with ActorLogging {

  import MasterActor._
  import context.dispatcher
  implicit val timeout: Timeout = Timeout(3 seconds)

  private val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(
      self,
      initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent],
      classOf[UnreachableMember]
    )
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  override def receive: Receive = handleClusterEvents(
    Map.empty[Address, ActorRef],
    ConsistentHashRing[ActorRef](replicas = 60)
  )

  private def handleClusterEvents(
      workers: Map[Address, ActorRef],
      hashRing: ConsistentHashRing[ActorRef]
  ): Receive = {
    case MemberUp(member) if member.hasRole("worker") =>
      val workerSelection =
        context.actorSelection(s"${member.address}/user/worker")
      workerSelection
        .resolveOne()
        .map(ref => (member.address, ref))
        .pipeTo(self)
    case MemberRemoved(member, _) =>
      val newHashRing = hashRing.removeNode(workers(member.address))
      assignSegments(newHashRing)
      context.become(handleClusterEvents(workers - member.address, newHashRing))
    case pair: (Address, ActorRef) =>
      val newHashRing = hashRing.addNode(pair._2)
      assignSegments(newHashRing)
      context.become(handleClusterEvents(workers + pair, newHashRing))
  }

  private def assignSegments(hashRing: ConsistentHashRing[ActorRef]): Unit = {
    val segmentAssignments = hashRing.getSegments
    segmentAssignments.foreach { case (worker, segments) =>
      worker ! SegmentAssignment(segments.toSet)
    }
  }

}

object MasterActor {
  case class SegmentAssignment(segment: Set[String])
}
