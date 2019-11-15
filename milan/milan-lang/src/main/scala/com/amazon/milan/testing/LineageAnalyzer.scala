package com.amazon.milan.testing

import com.amazon.milan.stream.unfold
import com.amazon.milan.types.LineageRecord

import scala.collection.mutable


/**
 * A utility for analyzing lineage records.
 *
 * @param records A set of lineage records.
 */
class LineageAnalyzer(records: TraversableOnce[LineageRecord]) {
  private val recordsList = records.toList

  // Generate a map of recordId to the lineage records associated with that record.
  private val recordsById = recordsList.groupBy(_.subjectRecordId)

  // Generate a map of streamId to the lineage records for that stream.
  private val recordsByStreamId = recordsList.groupBy(_.subjectStreamId)

  /**
   * Gets whether a record has an associated lineage record.
   *
   * @param recordId A record ID.
   * @return True if a lineage record is present for the record.
   */
  def hasLineage(recordId: String): Boolean = this.recordsById.contains(recordId)

  /**
   * Gets whether a record is descended from another record.
   *
   * @param lineageRecord    A [[LineageRecord]] for the target record.
   * @param ancestorStreamId The desired ancestor stream ID.
   * @param ancestorRecordId The desired ancestor record ID.
   * @return True if the target record is descended from the ancestor record according to the lineage information.
   */
  def isDescendedFrom(lineageRecord: LineageRecord, ancestorStreamId: String, ancestorRecordId: String): Boolean = {
    this.isDescendedFrom(lineageRecord.subjectStreamId, lineageRecord.subjectRecordId, ancestorStreamId, ancestorRecordId)
  }

  /**
   * Gets whether a record is descended from another record.
   *
   * @param streamId         The stream ID fo the target record.
   * @param recordId         The target record ID.
   * @param ancestorStreamId The desired ancestor stream ID.
   * @param ancestorRecordId The desired ancestor record ID.
   * @return True if the target record is descended from the ancestor record according to the lineage information.
   */
  def isDescendedFrom(streamId: String, recordId: String, ancestorStreamId: String, ancestorRecordId: String): Boolean = {
    this.findUpwards(streamId, recordId, _.sourceRecords.exists(r => r.streamId == ancestorStreamId && r.recordId == ancestorRecordId)).nonEmpty
  }

  /**
   * Find the first record upstream from a starting record that is derived from an ancestor record.
   *
   * @param startRecordId    The ID of the record where the search should start.
   * @param ancestorRecordId The ID of the ancestor record to search for.
   * @return The [[LineageRecord]] identifying the first record found that has the ancestor record as one of its source
   *         records, or None if no matching record was found.
   */
  def findFirstUpstreamDependent(startRecordStreamId: String, startRecordId: String, ancestorRecordId: String): Option[LineageRecord] = {
    this.findUpwards(startRecordStreamId, startRecordId, _.sourceRecords.exists(_.recordId == ancestorRecordId))
  }

  /**
   * Gets all [[LineageRecord]] records where the specified target record is a source record.
   *
   * @param streamId The stream ID of the target record.
   * @param recordId The target record ID.
   * @return An array of [[LineageRecord]] objects that specify the target record as a source record.
   */
  def getImmediateDescendants(streamId: String, recordId: String): Array[LineageRecord] = {
    this.recordsList.filter(_.sourceRecords.exists(r => r.streamId == streamId && r.recordId == recordId)).toArray
  }

  /**
   * Gets the [[LineageRecord]] for a record with the specified ID from the specified stream, or None if no such lineage
   * record exists.
   */
  private def getRecord(streamId: String, recordId: String): Option[LineageRecord] = {
    this.recordsById.get(recordId).flatMap(_.find(_.subjectStreamId == streamId))
  }

  /**
   * Perform a breadth-first search looking for a [[LineageRecord]] that matches a predicate.
   *
   * @param startRecordId The ID of the record where the search will start.
   * @param predicate     The predicate that identifies the record being searched for.
   * @return The first [[LineageRecord]] found that matches the predicate, or None if none is found.
   */
  private def findUpwards(startRecordStreamId: String, startRecordId: String, predicate: LineageRecord => Boolean): Option[LineageRecord] = {
    // Unfold starting from the start record.
    // The state being passed is the current queue of records to search, and the set of records that have already
    // been searched.
    // When a matching record is found it is put on the output, so the first record non-empty output is the record
    // we're looking for.
    unfold((List((startRecordStreamId, startRecordId)), Set.empty[(String, String)])) {
      case (searchQueue, searched) =>
        if (searched.contains(searchQueue.head)) {
          if (searchQueue.isEmpty) {
            None
          }
          else {
            Some((searchQueue.tail, searched), None)
          }
        }
        else {
          val (streamId, recordId) = searchQueue.head

          this.getRecord(streamId, recordId) match {
            case Some(lineageRecord) =>
              if (predicate(lineageRecord)) {
                // We found a lineage record that satisfies the predicate so output it.
                Some((searchQueue.tail, searched), Some(lineageRecord))
              }
              else {
                // We found a lineage record but it doesn't match the predicate, so continue the search up
                // the chain.
                val newSearched = searched + searchQueue.head
                val newQueue = searchQueue.tail ++ lineageRecord.sourceRecords.map(pointer => (pointer.streamId, pointer.recordId))
                Some((newQueue, newSearched), None)
              }

            case None =>
              // This record has no associated lineage record. Lineage records are only created for records
              // that Milan creates, so this must have been an external record.
              Some((searchQueue.tail, searched), None)
          }
        }
    }
      .find(_.isDefined)
      .flatten
  }
}
