package com.amazon.milan.types

import com.amazon.milan.SemanticVersion


/**
 * Contains lineage information for a record.
 *
 * @param recordId              The ID of this lineage record.
 * @param subjectStreamId       The ID of the stream containing the subject record.
 * @param subjectRecordId       The ID of the record that this lineage record refers to.
 * @param applicationInstanceId The ID of the application instance that produced the record.
 * @param componentId           The ID of the application component that produced the record.
 * @param componentVersion      The version of the application component that produced the record.
 * @param sourceRecords         Identifies records that were directly used in the creation of this record.
 */
case class LineageRecord(recordId: String,
                         subjectStreamId: String,
                         subjectRecordId: String,
                         applicationInstanceId: String,
                         componentId: String,
                         componentVersion: SemanticVersion,
                         sourceRecords: Array[RecordPointer]) {
}
