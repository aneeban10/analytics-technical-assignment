package com.dixa.analytics

import cats.effect.IO
import com.dixa.analytics.dao.IngestionDao
import com.dixa.analytics.model.*

object Processor:

  def processEvents(database: IngestionDao)(events: fs2.Stream[IO, ActionType[? <: Event]]): fs2.Stream[IO, Int] =
    events.flatMap:
      case ActionType.Insert(event) =>
        event match
          case conversation: Conversation => fs2.Stream.eval(database.insert(conversation))
          case message: Message           => fs2.Stream.eval(database.insert(message))
          case _                          => fs2.Stream.empty
      case _ => fs2.Stream.empty
