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

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class EchoServer {
	
	private ServerBootstrap	_bootstrap;
	private Channel	_channel;
	private int	_port;

	public EchoServer(int port) {
		_port = port;
		ChannelFactory factory = new NioServerSocketChannelFactory(
										Executors.newCachedThreadPool(),
										Executors.newCachedThreadPool());
				 
		_bootstrap = new ServerBootstrap(factory);
		_bootstrap.setOption("reuseAddress", true);
		_bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() {
				return Channels.pipeline(new EchoServerHandler());
			}
		});
	}

	public void bind() {
         _channel = _bootstrap.bind(new InetSocketAddress(_port));
	}
	
	public void unbind() {
		if (_channel != null) {
			_channel.close();			
		}
		_bootstrap.releaseExternalResources();
	}
	
	public class EchoServerHandler extends SimpleChannelHandler {
		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
	        e.getChannel().write(e.getMessage());
	    }
	}
	
}
