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
package com.ikanow.aleph2.analytics.r.services;

import java.io.InputStream;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.analytics.r.assets.BeFileInputReader;
import com.ikanow.aleph2.analytics.r.data_model.IParser;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

/** Parser for reading in JSON data
 * @author Alex
 */
public class BeJsonParser implements IParser {
	protected static final Logger logger = LogManager.getLogger(BeJsonParser.class);

	private ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	private JsonParser _parser = null;
	private JsonFactory _factory = null;
		
	@Override
	public boolean multipleRecordsPerFile(){
		return true;
	}

	@Override
	public Tuple2<Long, IBatchRecord> getNextRecord(long currentFileIndex,String fileName,  InputStream inStream) {
		Tuple2<Long, IBatchRecord> t2 = null;
		try {
			if (null == _factory) {
				_factory = _mapper.getFactory();
			}
			if (null == _parser) {
				_parser = _factory.createParser(inStream);
			}
			JsonToken token = _parser.nextToken();
			while ((token != JsonToken.START_OBJECT) && (token != null)) {
				token = _parser.nextToken();
			}
			if (null == token) {
				_parser = null;
				return null; //EOF
			}
			JsonNode node = _parser.readValueAsTree();
			
			t2 = new Tuple2<Long, IBatchRecord>(currentFileIndex, new BeFileInputReader.BatchRecord(node, null));
			return t2;
			
		} catch (Exception e) {
			// (this can often happen as an EOF condition)s
			_parser = null;
			return null; //EOF
		}
	}

}
