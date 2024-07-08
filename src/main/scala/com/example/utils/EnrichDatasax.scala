package com.example.utils

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{
  AsyncResultSet,
  BoundStatement,
  Row,
  Statement
}

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try}

object EnrichDatastax {

  implicit class RichResultSet(private val rs: AsyncResultSet) extends AnyVal {
    def single[R](implicit f: Row => Try[R]): Option[R] = {
      val r = rs.one()
      if (Option(r).isDefined) Some(parse(r)) else None
    }
    def fetchAll[R](implicit
        f: Row => Try[R],
        ec: ExecutionContext
    ): Future[List[R]] = {
      bringAll(rs, List.empty[Row]).map(_.map(parse(_)(f)))
    }
    private[this] def parse[R](r: Row)(implicit f: Row => Try[R]) = f(r) match {
      case Success(value) => value
      case Failure(ex)    => throw ex
    }

    private[this] def bringAll(currentRs: AsyncResultSet, list: List[Row])(
        implicit ec: ExecutionContext
    ): Future[List[Row]] = {
      if (currentRs.hasMorePages) {
        val cp = currentRs.currentPage().iterator().asScala.toList
        currentRs.fetchNextPage.toScala.flatMap(np => bringAll(np, list ++ cp))
      } else {
        Future.successful(
          list ++ currentRs.currentPage().iterator().asScala.toList
        )
      }
    }

    /** doesn't throw exception if some row parsing fails ;use it to filter
      * desired rows
      */
    def filter[R](implicit
        f: Row => Try[Option[R]],
        ec: ExecutionContext
    ): Future[List[R]] = {
      bringAll(rs, List.empty[Row]).map(_.flatMap { row =>
        f(row) match {
          case Success(r) => r
          case Failure(_) => None
        }
      })
    }
  }

  implicit class RichSession(private val session: CqlSession) extends AnyVal {

    def timedExecuteAsync(
        statement: Statement[_]
    ): Future[AsyncResultSet] = {
      session.executeAsync(statement).toScala
    }
  }

  implicit class RichBoundStatement(boundStatement: BoundStatement) {
    def setOptionalLong(name: String, optionalValue: Option[Long]) = {
      optionalValue
        .map(boundStatement.setLong(name, _))
        .getOrElse(boundStatement.setToNull(name))
    }

    def setOptionalInt(name: String, optionalValue: Option[Int]) = {
      optionalValue
        .map(boundStatement.setInt(name, _))
        .getOrElse(boundStatement.setToNull(name))
    }

    def setOptionalString(name: String, optionalValue: Option[String]) = {
      optionalValue
        .map(boundStatement.setString(name, _))
        .getOrElse(boundStatement.setToNull(name))
    }
  }
}
