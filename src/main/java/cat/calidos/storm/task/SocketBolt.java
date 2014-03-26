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
import backtype.storm.tuple.Tuple;

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
public class SocketBolt implements IBolt {
	private static final int	DEFAULT_CONNECT_TIMEOUT	= 12000;
	public static Logger LOG = LoggerFactory.getLogger(SocketBolt.class);
	private String	_host;
	private int	_port;
	private Channel	_channel;
	private ClientBootstrap	_bootstrap;

	public SocketBolt(String host, int port) {
		_host = host;
		_port = port;
	}
	
	
	
	
	public void prepare(Map stormConf, TopologyContext context,
	        			final OutputCollector collector) {
		// TODO: check thrown exceptions, if any
		// TODO: check thread allocation, probably want to minimise on a Storm cluster
		ChannelFactory factory = new NioClientSocketChannelFactory(
										Executors.newCachedThreadPool(),
										Executors.newCachedThreadPool());
		_bootstrap = new ClientBootstrap(factory);
		
	    //bootstrap.setPipelineFactory(new XXX());
		
		//TODO: check parameters
	    _bootstrap.setOption("tcpNoDelay", true);
	    _bootstrap.setOption("keepAlive", true);
	
	    ChannelFuture future = _bootstrap.connect(new InetSocketAddress(_host, _port));
	    
	    int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        Object connectTimeoutConfig = stormConf.get(Config.NIMBUS_TASK_LAUNCH_SECS);
        if (connectTimeoutConfig != null) {
        	connectTimeout = ((Number)connectTimeoutConfig).intValue()*1000;
        }
        
	    future.awaitUninterruptibly(connectTimeout);
	    if (!future.isSuccess()) {
	    	_bootstrap.releaseExternalResources();
	    	throw new RuntimeException("Could not connect to '"+_host+":"+_port, future.getCause());
	    }
	    _channel = future.getChannel();  
	}




	public void execute(Tuple arg0) {
	
		// TODO Auto-generated method stub
	
	}




	public void cleanup() {
		//TODO: flush all write futures?
		//TODO: check close best practices and set await value
		_channel.close().awaitUninterruptibly(1000);
		bootstrap.releaseExternalResources();
	} 
        

}