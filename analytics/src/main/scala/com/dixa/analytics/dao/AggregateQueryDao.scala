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

  def escalatedMessagesCount: IO[Long] =
    fr"""SELECT COUNT(*)
        |FROM messages
        |INNER JOIN (SELECT messages.conversation_id, MIN(messages.created_at) as first, MAX(messages.created_at) as last
        |FROM messages GROUP BY messages.conversation_id) grouped_messages
        |ON messages.conversation_id=grouped_messages.conversation_id
        |WHERE FLOOR(JULIANDAY(last) - JULIANDAY(first)) >= 1 AND messages.conversation_id NOT IN (
        |SELECT DISTINCT conversation_id FROM messages WHERE messages.direction='outbound') AND messages.conversation_id NOT IN (
        |SELECT DISTINCT conversation_id FROM converstaion_tags where tag_name='spam')
        |""".stripMargin
      .query[Long]
      .unique
      .transact(xa)

object AggregateQueryDao:

  def create: Resource[IO, AggregateQueryDao] = Database.create().map(AggregateQueryDao(_))
