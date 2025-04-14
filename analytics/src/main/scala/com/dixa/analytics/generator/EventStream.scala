package com.dixa.analytics.generator

import cats.effect.IO
import com.dixa.analytics.model.ActionType.*
import com.dixa.analytics.model.*

import java.time.Instant
import java.util.UUID
import scala.util.Random

object EventStream:

  final private val LIMIT  = 5000
  final private val random = new Random(42)

  def generateEventStream: fs2.Stream[IO, ActionType[? <: Event]] =
    fs2.Stream
      .emits(1 to LIMIT)
      .map(_ => generateConversation())
      .flatMap { c =>
        val conversation = Insert(c)

        val messages = fs2.Stream
          .emits(1 to (random.nextInt(5) + 1))
          .fold(List.empty[(Int, Instant)]) { case (acc, id) =>
            val lastTimestamp = acc.headOption.map(_._2).getOrElse(c.createdAt)
            val nextTimestamp = lastTimestamp.plusSeconds(random.between(0, 172800)) // 0-2 days in seconds
            acc :+ (id, nextTimestamp)
          }
          .flatMap(fs2.Stream.emits(_))
          .map { case (id, createdAt) =>
            generateMessage(c, id, createdAt, c.initialDirection)
          }
          .filter(_.body != "<masked>")
          .map(Insert(_))

        val tags = fs2.Stream
          .emits(0 to (random.nextInt(3) + 1))
          .flatMap { _ =>
            // Generate tag to be inserted first
            val insertTag = generateConversationTag(c, None)

            // Small change that the tag is later removed
            if random.nextInt(50) == 0 then
              // Generate delete with reference to the same tag name and using insert timestamp as base
              val deleteTag = generateConversationTag(
                conversation = c,
                tagToBeRemoved = Some(insertTag)
              )
              fs2.Stream(Insert(insertTag), Delete(deleteTag))
            else fs2.Stream(Insert(insertTag))
          }

        fs2.Stream.emit(conversation) ++ messages ++ tags
      }

  private def generateConversation(): Conversation =
    Conversation(
      id = math.abs(random.nextLong()),
      createdAt = Instant.now().minusSeconds(random.nextInt(365 * 24 * 60 * 60)),
      initialDirection = if random.nextBoolean() then Direction.Inbound else Direction.Outbound,
      channel = Channel.values(random.nextInt(Channel.values.length)),
      contact = UUID.randomUUID(),
      assignee = if random.nextBoolean() then Some(UUID.randomUUID()) else None
    )

  private def generateMessage(
      conversation: Conversation,
      id: Int,
      createdAt: Instant,
      initialDirection: Direction
  ): Message =
    Message(
      id = id,
      conversationId = conversation.id,
      createdAt = createdAt,
      direction =
        if id == 1 then initialDirection else if random.nextBoolean() then Direction.Inbound else Direction.Outbound,
      body = if random.nextInt(50) == 0 then generateText(maskedConversation = true) else generateText(),
      author = UUID.randomUUID()
    )

  private def generateConversationTag(
      conversation: Conversation,
      tagToBeRemoved: Option[ConversationTag] = None
  ): ConversationTag =
    val baseTime = tagToBeRemoved.map(_.createdAt).getOrElse(conversation.createdAt)
    val timeOffset =
      if tagToBeRemoved.nonEmpty then random.between(86401, 259200).toLong // Removal 1-3 days later
      else random.nextInt(86400).toLong                                    // Added within first day

    ConversationTag(
      tagId = tagToBeRemoved.map(_.tagId).getOrElse(UUID.randomUUID()),
      createdAt = baseTime.plus(timeOffset, java.time.temporal.ChronoUnit.SECONDS),
      conversationId = conversation.id,
      tagName = tagToBeRemoved.map(_.tagName).getOrElse(tags(random.nextInt(tags.length)))
    )

  private def generateText(maskedConversation: Boolean = false): String =
    val words = List("Hello", "Hi", "Thanks", "Please", "Help", "Support", "Issue", "Problem", "Fixed", "Done")
    if maskedConversation
    then "<masked>"
    else
      (1 to random.nextInt(5) + 3)
        .map(_ => words(random.nextInt(words.length)))
        .mkString(" ")

  private val tags = List("urgent", "spam", "important", "follow-up", "resolved")
