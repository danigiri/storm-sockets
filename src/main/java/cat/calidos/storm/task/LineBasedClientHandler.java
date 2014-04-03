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

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;


public class LineBasedClientHandler extends SimpleChannelUpstreamHandler {



	private SocketBolt	_socketBolt;

	public LineBasedClientHandler(SocketBolt socketBolt) {
		_socketBolt = socketBolt;
	}

	/* (non-Javadoc)
	* @see org.jboss.netty.channel.SimpleChannelUpstreamHandler#messageReceived(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.MessageEvent)
	*//////////////////////////////////////////////////////////////////////////////
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		_socketBolt.handleEmit((String)e.getMessage());
	}


}
