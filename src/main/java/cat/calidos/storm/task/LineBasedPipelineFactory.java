package cat.calidos.storm.task;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;


public class LineBasedPipelineFactory implements ChannelPipelineFactory {

private static final int	DEFAULT_FRAME_SIZE	= 16384;

private static final StringDecoder	_stringDecoder = new StringDecoder();	// shareable
private static final StringEncoder	_stringEncoder = new StringEncoder();	// shareable

private SocketBolt	_socketBolt;

	public LineBasedPipelineFactory(SocketBolt socketBolt) {
			_socketBolt = socketBolt;
	}

	public ChannelPipeline getPipeline() throws Exception {
		ChannelPipeline pipeline = Channels.pipeline();
	         
		boolean stripDelimiter = true;
	    pipeline.addLast("framer", new DelimiterBasedFrameDecoder(
	    								DEFAULT_FRAME_SIZE, 
	        		 					stripDelimiter, 
	        		 					Delimiters.lineDelimiter()));    
		pipeline.addLast("decoder", _stringDecoder);	// down
		pipeline.addLast("encoder", _stringEncoder);
	    pipeline.addLast("handler", new LineBasedClientHandler(_socketBolt));
  
	    return pipeline;
     }

}
