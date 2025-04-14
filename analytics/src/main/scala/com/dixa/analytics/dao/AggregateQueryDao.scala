package com.dixa.analytics.dao

import cats.effect.{IO, Resource}
import doobie.implicits.*
import doobie.syntax.string.*
import doobie.util.transactor.Transactor

class AggregateQueryDao private (xa: Transactor[IO]):

  def conversationCount: IO[Long] =
    fr"""SELECT COUNT(*) FROM conversations"""
      .query[Long]
      .unique
      .transact(xa)

  def conversationTagsCount: IO[Long] =
    fr"""SELECT COUNT(*) FROM converstaion_tags"""
      .query[Long]
      .unique
      .transact(xa)

  def messagesCount: IO[Long] =
    fr"""SELECT COUNT(*) FROM messages"""
      .query[Long]
      .unique
      .transact(xa)

  def maskedMessagesCount: IO[Long] =
    fr"""SELECT COUNT(*) FROM messages WHERE body='<masked>'"""
      .query[Long]
      .unique
      .transact(xa)

object AggregateQueryDao:

  def create: Resource[IO, AggregateQueryDao] = Database.create().map(AggregateQueryDao(_))
