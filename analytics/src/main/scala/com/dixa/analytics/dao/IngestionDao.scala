package com.dixa.analytics.dao

import cats.effect.IO
import cats.effect.kernel.Resource
import com.dixa.analytics.model.{Conversation, ConversationTag, Message}
import com.dixa.analytics.model.MetaInstances.given
import doobie.*
import doobie.implicits.*
import doobie.syntax.string.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class IngestionDao(xa: Transactor[IO]):

  private given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  def insert(conversation: Conversation): IO[Int] =
    fr"""INSERT INTO conversations (id, created_at, initial_direction, channel, contact, assignee)
        |VALUES (
        |  ${conversation.id},
        |  ${conversation.createdAt},
        |  ${conversation.initialDirection},
        |  ${conversation.channel},
        |  ${conversation.contact},
        |  ${conversation.assignee}
        |) ON CONFLICT DO NOTHING""".stripMargin.update.run
      .transact(xa)
      .exceptSqlState { sqlState =>
        logger.error(s"Failed to insert conversation: $conversation. SqlState: $sqlState") *> IO.pure(0)
      }

  def insert(conversationTag: ConversationTag): IO[Int] =
    fr"""INSERT INTO conversation_tags (tag_id, created_at, conversation_id, tag_name)
        |VALUES (
        |  ${conversationTag.tagId},
        |  ${conversationTag.createdAt},
        |  ${conversationTag.conversationId},
        |  ${conversationTag.tagName}
        |) ON CONFLICT DO NOTHING""".stripMargin.update.run
      .transact(xa)
      .exceptSqlState { sqlState =>
        logger.error(s"Failed to insert conversation_tag: $conversationTag. SqlState: $sqlState") *> IO.pure(0)
      }

  def insert(message: Message): IO[Int] =
    fr"""INSERT INTO messages (id, conversation_id, created_at, direction, body, author)
        |VALUES (
        |  ${message.id},
        |  ${message.conversationId},
        |  ${message.createdAt},
        |  ${message.direction},
        |  ${message.body},
        |  ${message.author}
        |) ON CONFLICT DO NOTHING""".stripMargin.update.run
      .transact(xa)
      .exceptSqlState { sqlState =>
        logger.error(s"Failed to insert message: $message. SqlState: $sqlState") *> IO.pure(0)
      }

  def delete(conversationTag: ConversationTag): IO[Int] =
    fr"""DELETE FROM conversation_tags
        |WHERE tag_id = ${conversationTag.tagId}
        |AND conversation_id = ${conversationTag.conversationId}
        |""".stripMargin.update.run
      .transact(xa)
      .exceptSqlState { sqlState =>
        logger.error(s"Failed to insert conversation_tag: $conversationTag. SqlState: $sqlState") *> IO.pure(0)
      }

object IngestionDao:

  def create: Resource[IO, IngestionDao] = Database.create(recreate = true).map(IngestionDao(_))
