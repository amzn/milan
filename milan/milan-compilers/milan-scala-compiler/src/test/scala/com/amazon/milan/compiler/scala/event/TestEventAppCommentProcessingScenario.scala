package com.amazon.milan.compiler.scala.event

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.compiler.scala.testing.EventAppTester
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang.Stream
import com.amazon.milan.lang.aggregation.{any, max, sum}
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import org.junit.Assert._
import org.junit.Test

import java.time.Instant


object TestEventAppCommentProcessingScenario {
  object CommentEventTypes {
    val CreateComment = "CreateComment"
    val UpVoteComment = "UpVoteComment"
    val DownVoteComment = "DownVoteComment"
    val DeleteComment = "DeleteComment"
    val EditComment = "EditComment"
  }


  case class CommentEvent(commentId: String,
                          eventTime: Instant,
                          eventType: String,
                          username: String,
                          data: String)


  case class Comment(commentId: String,
                     createTime: Instant,
                     username: String,
                     text: String,
                     upvotes: Int,
                     downvotes: Int,
                     deleted: Boolean)


  object UserCommentVotes {
    def fromEvent(commentEvent: CommentEvent): UserCommentVotes = {
      if (commentEvent.eventType == CommentEventTypes.UpVoteComment) {
        UserCommentVotes(commentEvent.commentId, commentEvent.username, commentEvent.eventTime, 1, 0)
      }
      else {
        UserCommentVotes(commentEvent.commentId, commentEvent.username, commentEvent.eventTime, 0, 1)
      }
    }
  }

  case class UserCommentVotes(commentId: String,
                              username: String,
                              updateTime: Instant,
                              upvotes: Int,
                              downvotes: Int)


  case class CommentVotes(commentId: String,
                          updateTime: Instant,
                          upvotes: Int,
                          downvotes: Int)

  /**
   * Creates a new [[Comment]] from a comment creation event.
   */
  def createComment(event: CommentEvent): Comment = {
    assert(event.eventType == CommentEventTypes.CreateComment)

    Comment(event.commentId, event.eventTime, event.username, event.data, 0, 0, deleted = false)
  }

  /**
   * Applies aggregated comment vote counts to a rendered comment.
   */
  def applyCommentVotes(comment: Comment, votes: CommentVotes): Comment = {
    if (votes == null) {
      comment
    }
    else {
      Comment(
        comment.commentId,
        comment.createTime,
        comment.username,
        comment.text,
        votes.upvotes,
        votes.downvotes,
        comment.deleted
      )
    }
  }

  /**
   * Updates a [[Comment]] using information from a comment event.
   */
  def updateComment(comment: Comment, event: CommentEvent): Comment = {
    if (event == null) {
      comment
    }
    else {
      event.eventType match {
        case CommentEventTypes.EditComment =>
          Comment(comment.commentId, comment.createTime, comment.username, event.data, comment.upvotes, comment.downvotes, comment.deleted)

        case CommentEventTypes.DeleteComment =>
          Comment(comment.commentId, comment.createTime, comment.username, comment.text, comment.upvotes, comment.downvotes, deleted = true)
      }
    }
  }
}

import com.amazon.milan.compiler.scala.event.TestEventAppCommentProcessingScenario._


@Test
class TestEventAppCommentProcessingScenario {
  @Test
  def test_EventApp_CommentProcessingScenario_CorrectlyHandlesEvents(): Unit = {
    // This test is a scenario where there some event-driven processing of user comments.
    // Comment events are the log of actions that users perform, and the output is rendered comment objects
    // that can be searched or displayed.

    val events = Stream.of[CommentEvent]

    // Create comments from create events.
    val createdComments =
      events
        .where(r => r.eventType == CommentEventTypes.CreateComment).withId("createCommentEvents")
        .map(r => TestEventAppCommentProcessingScenario.createComment(r)).withId("createdComments")

    // Apply upvotes and downvotes.
    // To do this, first we aggregate upvote and downvote counts for each comment.
    // Because users can change votes, we have to keep track of the latest vote for each user, and aggregate that
    // per comment.
    def getUserCommentVotes(items: Stream[CommentEvent]): Stream[UserCommentVotes] = {
      items.maxBy(r => r.eventTime).map(r => UserCommentVotes.fromEvent(r)).withId("userCommentVotes")
    }

    // A stream of the latest vote per (comment, user) pair.
    // We represent these as counts so we can sum them later.
    val latestVotePerUser = events
      .where(r => r.eventType == CommentEventTypes.UpVoteComment || r.eventType == CommentEventTypes.DownVoteComment).withId("voteEvents")
      .groupBy(r => (r.commentId, r.username)).withId("eventsGroupedByCommentIdAndUser")
      .flatMap((k, items) => getUserCommentVotes(items)).withId("latestVotePerUser")

    def maxUserCommentVotesByTime(userVotes: Stream[UserCommentVotes]): Stream[UserCommentVotes] = {
      userVotes.maxBy(r => r.updateTime).withId("userVotesMaxByTimeWithinGroup")
    }

    def getVotesPerComment(userVotes: Stream[UserCommentVotes]): Stream[CommentVotes] = {
      userVotes
        .groupBy(r => r.username).withId("userVotesGroupedByUser")
        .map((k, g) => maxUserCommentVotesByTime(g)).withId("userVotesMaxByTime")
        .recordWindow(1).withId("userVotesRecordWindow")
        .select(vote => CommentVotes(any(vote.commentId), max(vote.updateTime), sum(vote.upvotes), sum(vote.downvotes))).withId("userVotes")
    }

    // A stream of aggregated vote counts per comment.
    // Group by comment, and within the group take the latest votes per user and sum them up over all users.
    val votesPerComment = latestVotePerUser
      .groupBy(r => r.commentId).withId("latestVotesPerUserGroupedByCommentId")
      .flatMap((commentId, commentVotes) => getVotesPerComment(commentVotes)).withId("votesPerComment")

    // Join the vote counts with the rendered comments and set the counts.
    val commentsWithVotes = createdComments
      .fullJoin(votesPerComment)
      .where((comment, votes) => comment.commentId == votes.commentId)
      .select((comment, votes) => TestEventAppCommentProcessingScenario.applyCommentVotes(comment, votes))
      .withId("commentsWithVotes")

    // Apply comment edits, ensuring they are in temporal order.
    // We don't want to apply an edit older than the latest one.
    def maxByEventTime(events: Stream[CommentEvent]): Stream[CommentEvent] = {
      events.maxBy(r => r.eventTime).withId("eventsMaxByEventTime")
    }

    val editEventsByTime = events
      .where(r => r.eventType == CommentEventTypes.EditComment).withId("allEditEvents")
      .groupBy(r => r.commentId).withId("editEventsGroupedByCommentId")
      .flatMap((k, g) => maxByEventTime(g)).withId("editEventsByTime")

    val commentsWithEdits = commentsWithVotes
      .fullJoin(editEventsByTime)
      .where((l, r) => l.commentId == r.commentId)
      .select((comment, event) => TestEventAppCommentProcessingScenario.updateComment(comment, event)).withId("commentsWithEdits")

    // Deletes can't be undone so we don't care about the order we process them in.
    val deleteEvents = events.where(r => r.eventType == CommentEventTypes.DeleteComment).withId("deleteEvents")

    val commentsWithDeletes = commentsWithEdits
      .fullJoin(deleteEvents)
      .where((l, r) => l.commentId == r.commentId)
      .select((comment, event) => TestEventAppCommentProcessingScenario.updateComment(comment, event)).withId("commentsWithDeletes")

    val config = new ApplicationConfiguration()
    val sink = config.addMemorySink(commentsWithDeletes)
    val consumer = EventAppTester.compile(commentsWithDeletes, config)

    val t0 = Instant.now()
    consumer.consume(events.streamId, CommentEvent("c1", t0, CommentEventTypes.CreateComment, "u1", "text"))
    assertEquals(1, sink.getRecordCount)
    val comment1 = sink.getValues.last
    assertEquals("text", comment1.text)
    assertEquals(0, comment1.upvotes)

    consumer.consume(events.streamId, CommentEvent("c1", t0.plusSeconds(1), CommentEventTypes.UpVoteComment, "u2", ""))
    assertEquals(2, sink.getRecordCount)
    val comment2 = sink.getValues.last
    assertEquals(1, comment2.upvotes)

    consumer.consume(events.streamId, CommentEvent("c1", t0.plusSeconds(2), CommentEventTypes.DownVoteComment, "u2", ""))
    assertEquals(3, sink.getRecordCount)
    val comment3 = sink.getValues.last
    assertEquals(0, comment3.upvotes)
    assertEquals(1, comment3.downvotes)

    consumer.consume(events.streamId, CommentEvent("c1", t0.plusSeconds(3), CommentEventTypes.UpVoteComment, "u3", ""))
    assertEquals(4, sink.getRecordCount)
    val comment4 = sink.getValues.last
    assertEquals(1, comment4.upvotes)
    assertEquals(1, comment4.downvotes)

    consumer.consume(events.streamId, CommentEvent("c1", t0.plusSeconds(4), CommentEventTypes.EditComment, "u1", "new text"))
    assertEquals(5, sink.getRecordCount)
    val comment5 = sink.getValues.last
    assertEquals("new text", comment5.text)

    consumer.consume(events.streamId, CommentEvent("c1", t0.plusSeconds(3), CommentEventTypes.EditComment, "u1", "ignore me"))
    assertEquals(6, sink.getRecordCount)
    val comment6 = sink.getValues.last
    assertEquals("new text", comment6.text)
  }

  @Test
  def test_EventApp_CommentProcessingScenario_HasCorrectKeys(): Unit = {
    // This test is a scenario where there some event-driven processing of user comments.
    // Comment events are the log of actions that users perform, and the output is rendered comment objects
    // that can be searched or displayed.

    val events = Stream.of[CommentEvent]

    // Create comments from create events.
    val createdComments =
      events
        .where(r => r.eventType == CommentEventTypes.CreateComment).withId("createCommentEvents")
        .map(r => TestEventAppCommentProcessingScenario.createComment(r)).withId("createdComments")

    // Apply upvotes and downvotes.
    // To do this, first we aggregate upvote and downvote counts for each comment.
    // Because users can change votes, we have to keep track of the latest vote for each user, and aggregate that
    // per comment.
    def getUserCommentVotes(items: Stream[CommentEvent]): Stream[UserCommentVotes] = {
      items.maxBy(r => r.eventTime).map(r => UserCommentVotes.fromEvent(r)).withId("userCommentVotes")
    }

    // A stream of the latest vote per (comment, user) pair.
    // We represent these as counts so we can sum them later.
    val latestVotePerUser = events
      .where(r => r.eventType == CommentEventTypes.UpVoteComment || r.eventType == CommentEventTypes.DownVoteComment).withId("voteEvents")
      .groupBy(r => (r.commentId, r.username)).withId("eventsGroupedByCommentIdAndUser")
      .flatMap((k, items) => getUserCommentVotes(items)).withId("latestVotePerUser")

    def maxUserCommentVotesByTime(userVotes: Stream[UserCommentVotes]): Stream[UserCommentVotes] = {
      userVotes.maxBy(r => r.updateTime).withId("userVotesMaxByTimeWithinGroup")
    }

    def getVotesPerComment(userVotes: Stream[UserCommentVotes]): Stream[CommentVotes] = {
      userVotes
        .groupBy(r => r.username).withId("userVotesGroupedByUser")
        .map((k, g) => maxUserCommentVotesByTime(g)).withId("userVotesMaxByTime")
        .recordWindow(1).withId("userVotesRecordWindow")
        .select(vote => CommentVotes(any(vote.commentId), max(vote.updateTime), sum(vote.upvotes), sum(vote.downvotes))).withId("userVotes")
    }

    // A stream of aggregated vote counts per comment.
    // Group by comment, and within the group take the latest votes per user and sum them up over all users.
    val votesPerComment = latestVotePerUser
      .groupBy(r => r.commentId).withId("latestVotesPerUserGroupedByCommentId")
      .flatMap((commentId, commentVotes) => getVotesPerComment(commentVotes)).withId("votesPerComment")

    // Join the vote counts with the rendered comments and set the counts.
    val commentsWithVotes = createdComments
      .fullJoin(votesPerComment)
      .where((comment, votes) => comment.commentId == votes.commentId)
      .select((comment, votes) => TestEventAppCommentProcessingScenario.applyCommentVotes(comment, votes))
      .withId("commentsWithVotes")

    // Apply comment edits, ensuring they are in temporal order.
    // We don't want to apply an edit older than the latest one.
    def maxByEventTime(events: Stream[CommentEvent]): Stream[CommentEvent] = {
      events.maxBy(r => r.eventTime).withId("eventsMaxByEventTime")
    }

    val editEventsByTime = events
      .where(r => r.eventType == CommentEventTypes.EditComment).withId("allEditEvents")
      .groupBy(r => r.commentId).withId("editEventsGroupedByCommentId")
      .flatMap((k, g) => maxByEventTime(g)).withId("editEventsByTime")

    val commentsWithEdits = commentsWithVotes
      .fullJoin(editEventsByTime)
      .where((l, r) => l.commentId == r.commentId)
      .select((comment, event) => TestEventAppCommentProcessingScenario.updateComment(comment, event)).withId("commentsWithEdits")

    // Deletes can't be undone so we don't care about the order we process them in.
    val deleteEvents = events.where(r => r.eventType == CommentEventTypes.DeleteComment).withId("deleteEvents")

    val commentsWithDeletes = commentsWithEdits
      .fullJoin(deleteEvents)
      .where((l, r) => l.commentId == r.commentId)
      .select((comment, event) => TestEventAppCommentProcessingScenario.updateComment(comment, event)).withId("commentsWithDeletes")

    val config = new ApplicationConfiguration()
    config.addMemorySink(commentsWithDeletes)

    val generatedStreams = EventAppTester.compileStreams(StreamCollection.build(commentsWithDeletes), config)

    val editEventsByTimeInfo = generatedStreams.getStream("editEventsByTime")
    assertEquals(types.EmptyTuple, editEventsByTimeInfo.fullKeyType)

    val commentsWithDeletesInfo = generatedStreams.getStream("commentsWithDeletes")
    assertEquals(types.EmptyTuple, commentsWithDeletesInfo.fullKeyType)

    val userVotesMaxByTimeWithinGroupInfo = generatedStreams.getStream("userVotesMaxByTimeWithinGroup")
    assertEquals(TypeDescriptor.of[Tuple1[String]], userVotesMaxByTimeWithinGroupInfo.contextKeyType)
    assertEquals(types.String, userVotesMaxByTimeWithinGroupInfo.keyType)
  }
}
