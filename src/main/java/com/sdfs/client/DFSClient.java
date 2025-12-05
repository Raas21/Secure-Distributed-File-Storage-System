package com.sdfs.client;

import com.sdfs.protocol.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DFSClient {
    private final String host;
    private final int port;

    // We use a Queue to wait for the async Netty response in the main thread
    private final BlockingQueue<SDFSResponse> responseQueue = new LinkedBlockingQueue<>();

    public DFSClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start() {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
                            ch.pipeline().addLast(new ProtobufDecoder(SDFSResponse.getDefaultInstance()));
                            ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
                            ch.pipeline().addLast(new ProtobufEncoder());
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<SDFSResponse>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, SDFSResponse msg) {
                                    responseQueue.offer(msg); // Pass response to main thread
                                }
                            });
                        }
                    });

            Channel channel = b.connect(host, port).sync().channel();
            runInteractiveShell(channel); // Enter the command loop

            channel.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            group.shutdownGracefully();
        }
    }

    private void runInteractiveShell(Channel channel) throws InterruptedException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("--- S-DFS Client Started ---");
        System.out.println("Commands: create <path> <filename>, exit");

        while (true) {
            System.out.print("sdfs> ");
            String line = scanner.nextLine();
            if (line.equals("exit"))
                break;

            String[] parts = line.split(" ");
            if (parts[0].equals("create") && parts.length == 3) {
                sendCreateRequest(channel, parts[1], parts[2]);
            } else {
                System.out.println("Unknown command.");
            }
        }
    }

    private void sendCreateRequest(Channel channel, String path, String filename) throws InterruptedException {
        // 1. Build Request
        CreateFileRequest create = CreateFileRequest.newBuilder()
                .setPath(path)
                .setFilename(filename)
                .setOwnerId("root") // Hardcoded for now
                .setFileSize(0)
                .build();

        SDFSRequest request = SDFSRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setCreateFile(create)
                .build();

        // 2. Send
        channel.writeAndFlush(request);

        // 3. Wait for Response (Sync)
        SDFSResponse response = responseQueue.poll(5, TimeUnit.SECONDS);

        if (response == null) {
            System.err.println("Error: Timeout waiting for NameNode.");
        } else if (response.hasCreateFile()) {
            CreateFileResponse res = response.getCreateFile();
            if (res.getSuccess()) {
                System.out.println("Success! File ID: " + res.getFileId());
            } else {
                System.err.println("Failed: " + res.getErrorMessage());
            }
        } else {
            System.err.println("Error: Invalid response type.");
        }
    }

    public static void main(String[] args) {
        new DFSClient("localhost", 5001).start();
    }
}