package com.sdfs.namenode;

import com.sdfs.namenode.metadata.MetadataStore;
import com.sdfs.protocol.SDFSRequest;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class NameNodeServer {

    private final int port;
    // The Single Source of Truth for File System State
    private final MetadataStore metadataStore;

    public NameNodeServer(int port) {
        this.port = port;
        this.metadataStore = new MetadataStore();
    }

    public void run() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();

                            p.addLast(new ProtobufVarint32FrameDecoder());

                            // CHANGED: Now we decode the Wrapper (SDFSRequest)
                            p.addLast(new ProtobufDecoder(SDFSRequest.getDefaultInstance()));

                            p.addLast(new ProtobufVarint32LengthFieldPrepender());
                            p.addLast(new ProtobufEncoder());

                            // CHANGED: Inject the MetadataStore into the handler
                            p.addLast(new NameNodeHandler(metadataStore));
                        }
                    });

            ChannelFuture f = b.bind(port).sync();
            System.out.println("[NameNode] Started on port " + port);
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        int port = 5001;
        new NameNodeServer(port).run();
    }
}