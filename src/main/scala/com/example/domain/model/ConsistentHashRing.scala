package com.example.domain.model

import scala.collection.immutable.TreeMap
import scala.util.hashing.MurmurHash3

case class ConsistentHashRing[T](
    ring: TreeMap[Int, T] = TreeMap.empty[Int, T],
    replicas: Int = 100
) {

  def addNode(node: T): ConsistentHashRing[T] = {
    val updatedRing = (0 until replicas).foldLeft(ring) { (r, i) =>
      r + (hash(node.toString + i) -> node)
    }
    copy(ring = updatedRing)
  }

  def removeNode(node: T): ConsistentHashRing[T] = {
    val updatedRing = (0 until replicas).foldLeft(ring) { (r, i) =>
      r - hash(node.toString + i)
    }
    copy(ring = updatedRing)
  }

  def getNode(key: String): Option[T] = {
    if (ring.isEmpty) None
    else {
      val hashKey = hash(key)
      val tailMap = ring.rangeFrom(hashKey)
      val firstNode = if (tailMap.isEmpty) ring.head._2 else tailMap.head._2
      Some(firstNode)
    }
  }

  def getSegments: Map[T, Seq[String]] = {
    val segments = (0 until replicas).map(_.toString)
    segments
      .flatMap(segment => getNode(segment).map(node => node -> segment))
      .groupBy(_._1)
      .view
      .mapValues(_.map(_._2))
      .toMap
  }

  private def hash(key: String): Int = {
    MurmurHash3.stringHash(key)
  }
}
