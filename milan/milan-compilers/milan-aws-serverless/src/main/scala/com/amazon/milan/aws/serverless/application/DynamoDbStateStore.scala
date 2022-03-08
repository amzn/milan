package com.amazon.milan.aws.serverless.application

import com.amazon.milan.application.StateStore
import com.amazon.milan.program.FunctionDef
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * A [[StateStore]] that specifies that state should be stored in a DynamoDb table.
 *
 * @param encoder   A [[FunctionDef]] defining a function that converts raw state values to database records.
 * @param decoder   A [[FunctionDef]] defining a function that converts from database records to state values.
 * @param tableName The name of the DynamoDb table where state values will be stored, or None if the table name will
 *                  be provided at runtime.
 */
@JsonSerialize
@JsonDeserialize
class DynamoDbStateStore(val encoder: FunctionDef,
                         val decoder: FunctionDef,
                         val tableName: Option[String] = None) extends StateStore {
}
