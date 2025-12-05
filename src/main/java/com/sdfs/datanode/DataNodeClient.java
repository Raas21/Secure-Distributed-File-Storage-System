package com.sdfs.datanode;

import com.sdfs.protocol.SDFSResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

public class DataNodeClient {

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 5001; // Must match NameNode port

        EventLoopGroup group = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
                            // CAREFUL: Client decodes RESPONSES, not Requests
                            ch.pipeline().addLast(new ProtobufDecoder(SDFSResponse.getDefaultInstance()));

                            ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
                            ch.pipeline().addLast(new ProtobufEncoder());

                            ch.pipeline().addLast(new DataNodeHandler());
                        }
                    });

            // Start the connection attempt
            ChannelFuture f = b.connect(host, port).sync();

            // Wait until the connection is closed
            f.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}