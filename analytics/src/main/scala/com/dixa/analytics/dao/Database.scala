package com.dixa.analytics.dao

import cats.effect.IO
import cats.effect.kernel.Resource
import doobie.*
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.file.{Files, Paths}

object Database:

  private given logger: Logger[IO] = Slf4jLogger.getLogger[IO]
  final private val DATABASE_FILE  = "database.db"

  def create(recreate: Boolean = false): Resource[IO, Transactor[IO]] = for
    ec <- ExecutionContexts.fixedThreadPool[IO](4)
    // No credentials required by SQLite
    transactor <- HikariTransactor.newHikariTransactor[IO](
      "org.sqlite.JDBC",
      s"jdbc:sqlite:$DATABASE_FILE",
      "",
      "",
      ec
    )
    _ <- Resource.eval(if recreate then recreateDatabase(transactor) else IO.unit)
  yield transactor

  private def recreateDatabase(transactor: Transactor[IO]): IO[Unit] =
    IO.blocking(Files.deleteIfExists(Paths.get(DATABASE_FILE))).void >>
      logger.info(s"Recreated the database file '$DATABASE_FILE'") >>
      createTables(transactor) >>
      logger.info("Created database tables")

  private def createConversationTable(xa: Transactor[IO]): IO[Int] =
    fr"""CREATE TABLE IF NOT EXISTS conversations (
        |  id INTEGER PRIMARY KEY,
        |  created_at TIMESTAMP NOT NULL,
        |  initial_direction TEXT NOT NULL,
        |  channel TEXT NOT NULL,
        |  contact TEXT NOT NULL,
        |  assignee UUID
        |)""".stripMargin.update.run.transact(xa)

  private def createConversationTagTable(xa: Transactor[IO]): IO[Int] =
    fr"""CREATE TABLE IF NOT EXISTS conversation_tags (
        |  tag_id UUID NOT NULL,
        |  created_at TIMESTAMP NOT NULL,
        |  conversation_id INTEGER NOT NULL,
        |  tag_name TEXT NOT NULL,
        |
        |  PRIMARY KEY (tag_id, conversation_id),
        |  FOREIGN KEY (conversation_id) REFERENCES conversations(id)
        |)""".stripMargin.update.run.transact(xa)

  private def createMessageTable(xa: Transactor[IO]): IO[Int] =
    fr"""CREATE TABLE IF NOT EXISTS messages (
        |  id INTEGER NOT NULL,
        |  conversation_id INTEGER NOT NULL,
        |  created_at TIMESTAMP NOT NULL,
        |  direction TEXT NOT NULL,
        |  body TEXT NOT NULL,
        |  author UUID NOT NULL,
        |
        |  PRIMARY KEY (id, conversation_id),
        |  FOREIGN KEY (conversation_id) REFERENCES conversations(id)
        |)""".stripMargin.update.run.transact(xa)

  private def createTables(xa: Transactor[IO]): IO[Unit] = for {
    _ <- createConversationTable(xa)
    _ <- createConversationTagTable(xa)
    _ <- createMessageTable(xa)
  } yield ()
