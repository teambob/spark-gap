/*
Copyright 2020 Andrew Punch

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */


package com.github.teambob.sparkgap

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StringType

object udf {
  /**
   * Try to get a field from a StructType column. If the field is not present then
   * null will be returned. The field will be returned as a string
   *
   * https://stackoverflow.com/a/57270633/141736
   *
   * param root Column
   * param fieldName Name of field in StructType
   * return contents of struct field as a string, or null if not present
   */
  val tryGet: (GenericRowWithSchema, String) => String = (root, fieldName) => {
    val buffer: Row = root
    if (buffer!=null) {
      if (buffer.schema.contains(fieldName)) {
        val fieldIndex = buffer.fieldIndex(fieldName)
        if (buffer.schema.fields(fieldIndex).dataType.isInstanceOf[StringType]) {
          buffer.getString(fieldIndex)
        } else {
          buffer.get(fieldIndex).toString
        }
      }
    }
    null
  }: String

}
