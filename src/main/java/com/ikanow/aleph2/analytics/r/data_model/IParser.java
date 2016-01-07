/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.ikanow.aleph2.analytics.r.data_model;

import java.io.InputStream;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;

public interface IParser {

	/** Get the next record in a list
	 *  Returns null when done
	 * @param currentFileIndex
	 * @param fileName
	 * @param inStream
	 * @return
	 */
	Tuple2<Long, IBatchRecord> getNextRecord(long currentFileIndex, String fileName, InputStream inStream);
	
	/** Returns true if a file can contain multiple records 
	 * @return
	 */
	default boolean multipleRecordsPerFile(){
		return false;
	}

}
