package com.amazon.milan.aws.serverless.application

import com.amazon.milan.application.StateStore
import com.amazon.milan.program.FunctionDef
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * A [[StateStore]] that specifies that state should be stored in a DynamoDb table.
 *
 * @param tableName The name of the DynamoDb table where state values will be stored.
 * @param encoder   A [[FunctionDef]] defining a function that converts raw state values to database records.
 * @param decoder   A [[FunctionDef]] defining a function that converts from database records to state values.
 */
@JsonSerialize
@JsonDeserialize
class DynamoDbStateStore(val tableName: String,
                         val encoder: FunctionDef,
                         val decoder: FunctionDef) extends StateStore {
}
