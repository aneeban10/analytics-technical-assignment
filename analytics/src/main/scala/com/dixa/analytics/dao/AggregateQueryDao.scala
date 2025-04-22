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
    fr"""SELECT COUNT(*) FROM conversation_tags"""
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
    fr"""WITH non_spam_conversations AS (
        | SELECT conversation_id FROM conversation_tags WHERE tag_name != 'spam'),
        |
        |inbound_messages AS (
        | SELECT id, created_at, conversation_id FROM messages m
        | WHERE direction='inbound' AND conversation_id IN (SELECT conversation_id FROM non_spam_conversations)),
        |
        |ranked_messages AS (
        | SELECT
        |   id,
        |   created_at,
        |   conversation_id,
        |   LAG(created_at) OVER (PARTITION BY conversation_id ORDER BY created_at) AS prev_created_at
        | FROM inbound_messages),
        |
        |valid_messages AS (
        | SELECT *
        | FROM ranked_messages rm
        | WHERE rm.prev_created_at IS NULL
        |   OR (
        |     FLOOR(julianday(rm.created_at) - julianday(rm.prev_created_at)) >= 1
        |     AND NOT EXISTS (
        |       SELECT 1 FROM messages o
        |       WHERE o.direction = 'outbound'
        |         AND o.conversation_id = rm.conversation_id
        |         AND o.created_at > rm.prev_created_at
        |         AND o.created_at < rm.created_at)))
        |
        |SELECT COUNT(DISTINCT conversation_id) AS valid_conversation_count FROM valid_messages;
        |""".stripMargin
      .query[Long]
      .unique
      .transact(xa)

object AggregateQueryDao:

  def create: Resource[IO, AggregateQueryDao] = Database.create().map(AggregateQueryDao(_))
