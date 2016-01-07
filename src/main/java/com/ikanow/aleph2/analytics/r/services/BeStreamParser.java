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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.analytics.r.assets.BeFileInputReader;
import com.ikanow.aleph2.analytics.r.data_model.IParser;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

/** Parser for readining in binary data
 * @author Alex
 */
public class BeStreamParser implements IParser {

	private static final Logger logger = LogManager.getLogger(BeStreamParser.class);
	
	@Override
	public Tuple2<Long, IBatchRecord> getNextRecord(long currentFileIndex,String fileName,  InputStream inStream) {
		logger.debug("StreamParser.getNextRecord");

		ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		Tuple2<Long, IBatchRecord> t2 = null;
		try {
		   JsonNode node = mapper.createObjectNode(); 
		   ((ObjectNode) node).put("fileName", fileName);
		   // create output stream
			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
	        int readedBytes;
	        byte[] buf = new byte[1024];
	        while ((readedBytes = inStream.read(buf)) > 0)
	        {
	            outStream.write(buf, 0, readedBytes);
	        }
	        outStream.close();			    	
			t2 = new Tuple2<Long, IBatchRecord>(currentFileIndex, new BeFileInputReader.BatchRecord(node, outStream));
		} catch (Exception e) {
			logger.error("JsonParser caught exception",e);
		}
		return t2;
	}

}
