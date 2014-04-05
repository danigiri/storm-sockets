/**
 Copyright 2014 Daniel Giribet <dani - calidos.cat>

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
package cat.calidos.storm.task.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import cat.calidos.storm.task.SocketBolt;


public class SocketBoltTest {

	private EchoServer	_echoServer;
	private SocketBolt	_socketBolt;

	@Before 
	public void setUp() {
		_echoServer = new EchoServer(1111);
		_echoServer.bind();
	}

	@After
	public void tearDown() {
		_echoServer.unbind();
	}

	@Test
	public void testSocketBolt() throws Exception {
	_socketBolt = new SocketBolt("localhost", 1111);

	HashMap<String, Object> stormConf = new HashMap<String, Object>(0);
	MemoryOutputCollector collector = new MemoryOutputCollector();
	OutputCollector outputCollector = new OutputCollector(collector);
		
	_socketBolt.prepare(stormConf, null, outputCollector);
	
	int taskId = 1;
	Map<Integer, String> taskToComponent = new HashMap<Integer, String>(1);
	taskToComponent.put(taskId, "ComponentId");
	
	String streamId = Utils.DEFAULT_STREAM_ID;
	HashMap<String, Fields> fields = new HashMap<String, Fields>(1);
	fields.put(streamId, new Fields("output"));
	Map<String, Map<String, Fields>> componentToStreamToFields = new HashMap<String, Map<String, Fields>>(1);
	componentToStreamToFields.put("ComponentId", fields);
	GeneralTopologyContext context = new GeneralTopologyContext(null, null, taskToComponent, null, componentToStreamToFields, "stormId");

	List<Object> values = new Values("one");
	TupleImpl tuple = new TupleImpl(context, values, taskId, streamId);
	
	_socketBolt.execute(tuple);
	_socketBolt.execute(tuple);
	_socketBolt.execute(tuple);
	Thread.sleep(1000);
	_socketBolt.cleanup();
	
	List<Object> processedValues = collector.getValues();
	assertNotNull("", processedValues);
	assertEquals("", 3, processedValues.size());
	assertEquals("", values, processedValues.get(0));
	assertEquals("", values, processedValues.get(1));
	assertEquals("", values, processedValues.get(2));
	
	}

}
