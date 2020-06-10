package com.amazon.milan.types


/**
 * A reference to a record on a stream.
 *
 * @param streamId The unique ID of a stream.
 * @param recordId The unique ID of a record.
 */
case class RecordPointer(streamId: String, recordId: String)
