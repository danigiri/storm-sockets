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
package cat.calidos.storm.task;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A bolt that connects to a process using sockets.
 * 
 * <p>To run a SocketBolt on a cluster, there needs to be TCP access from all the
 * cluster nodes</p>
 * 
 * <p>When creating topologies using the Java API, subclass this bot and implement
 * the IRichBolt interface. TODO: establish subclassing and protocol interfaces if needed
 * 
 * @author daniel giribet
 */
public class SocketBolt extends BaseRichBolt implements IBolt {
	private static final String	CRLF	= "\r\n";

	private static final int	DEFAULT_CONNECT_TIMEOUT	= 12000;
	
	public static Logger LOG = LoggerFactory.getLogger(SocketBolt.class);
	private String _host;
	private int	_port;
	private Channel	_channel;
	private Map _options;
	private ClientBootstrap	_bootstrap;
	private OutputCollector	_collector;

	private ChannelFuture	_writeFuture;

	public SocketBolt(String host, int port) {
		_host = host;
		_port = port;
		_options = new HashMap(2);
		_options.put("tcpNoDelay", true);
		_options.put("keepAlive", true);
	}
	
	public void prepare(Map stormConf, TopologyContext context,
	        			final OutputCollector collector) {
		_collector = collector;
		
		// TODO: check if we have to handle thrown exceptions when connecting, if any
		// TODO: double-check thread allocation, NIO is probably handling this asynchronously
		ChannelFactory factory = new NioClientSocketChannelFactory(
										Executors.newCachedThreadPool(),
										Executors.newCachedThreadPool());
		_bootstrap = new ClientBootstrap(factory);
		
		_bootstrap.setPipelineFactory(new LineBasedPipelineFactory(this));
	    
		_bootstrap.setOptions(_options);
		
	    ChannelFuture future = _bootstrap.connect(new InetSocketAddress(_host, _port));
	    
	    int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        Object connectTimeoutConfig = stormConf.get(Config.NIMBUS_TASK_LAUNCH_SECS);
        if (connectTimeoutConfig != null) {
        	connectTimeout = ((Number)connectTimeoutConfig).intValue()*1000/2;
        }
        
	    future.awaitUninterruptibly(connectTimeout);
	    if (!future.isSuccess()) {
	    	_bootstrap.releaseExternalResources();
	    	throw new RuntimeException("Could not connect to '"+_host+":"+_port, future.getCause());
	    }
	    _channel = future.getChannel();  
	}

	public void execute(Tuple input) {
	
		_writeFuture = _channel.write(input.getValue(0) + CRLF);
			
	}

	public void handleEmit(String message) {
	
		_collector.emit(new Values(message));
	}

	public void cleanup() {

//TODO: double check: it seems silly to wait for the last write when cleaning up, we can't really emit results
//		if (_writeFuture != null) {
//			
//		}
		//TODO: check close best practices and set appropriate await value
		_channel.close().awaitUninterruptibly(1000);
		_bootstrap.releaseExternalResources();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("output"));
	}
 

}