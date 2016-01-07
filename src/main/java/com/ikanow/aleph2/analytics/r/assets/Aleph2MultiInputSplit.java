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
package com.ikanow.aleph2.analytics.r.assets;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

/** Encapsulates another input format class
 * @author Alex
 */
@SuppressWarnings("rawtypes")
public class Aleph2MultiInputSplit extends InputSplit implements Configurable, Writable {

	protected String _name;
	protected InputSplit _delegate;
	protected Class<? extends InputSplit> _input_split;
	protected Class<? extends InputFormat> _input_format;
	protected Class<? extends Mapper> _mapper;
	
	protected Configuration _conf;
	
	public InputSplit getInputSplit() { return _delegate; }
	public Class<? extends InputFormat> getInputFormatClass() { return _input_format; }
	public Class<? extends Mapper> getMapperClass() { return _mapper; }
	public String getName() { return _name; }
	
	public Aleph2MultiInputSplit() {}
	
	public Aleph2MultiInputSplit(String name, InputSplit delegate,
			Configuration config,
			Class<? extends InputFormat> input_format, Class<? extends Mapper> mapper_delegate)
	{
		_name = name;
		_delegate = delegate;
		_conf = config;
		_input_split = delegate.getClass();
		_input_format = input_format;		
		_mapper = mapper_delegate;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		_name = Text.readString(in);
		_input_split = (Class<? extends InputSplit>) readClass(in);
		_input_format = (Class<? extends InputFormat<?, ?>>) readClass(in);
		_mapper = (Class<? extends Mapper<?, ?, ?, ?>>) readClass(in);
		_delegate = (InputSplit) ReflectionUtils.newInstance(_input_split, _conf);
		 final SerializationFactory factory = new SerializationFactory(_conf);
		 final Deserializer deserializer = factory.getDeserializer(_input_split);
		 deserializer.open((DataInputStream)in);
		 _delegate = (InputSplit)deserializer.deserialize(_delegate);		
	}
	private Class<?> readClass(DataInput in) throws IOException {
		String className = Text.readString(in);
		try {
			return _conf.getClassByName(className);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("readObject can't find class", e);
		}
	}	
	
	@SuppressWarnings("unchecked")
	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, _name);
	    Text.writeString(out, _input_split.getName());
	    Text.writeString(out, _input_format.getName());
	    Text.writeString(out, _mapper.getName());
	    final SerializationFactory factory = new SerializationFactory(_conf);
	    final Serializer serializer = factory.getSerializer(_input_split);
	    serializer.open((DataOutputStream)out);
	    serializer.serialize(_delegate);		
	}

	@Override
	public Configuration getConf() {
		return _conf;
	}

	@Override
	public void setConf(Configuration conf) {
		_conf = conf;		
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return _delegate.getLength();
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return _delegate.getLocations();
	}

	@Override
	public String toString() {
		return Aleph2MultiInputSplit.class.getSimpleName() + ":" + Optional.ofNullable(_name).orElse("(no name)") + ":" +  Optional.ofNullable(_delegate).map(d -> d.toString()).orElse("none");
	}
}
