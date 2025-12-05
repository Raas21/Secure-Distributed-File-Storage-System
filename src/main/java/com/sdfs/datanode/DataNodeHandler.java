package com.sdfs.datanode;

import com.sdfs.protocol.HeartBeatRequest;
import com.sdfs.protocol.SDFSRequest; // Import the Wrapper
import com.sdfs.protocol.SDFSResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.UUID;

public class DataNodeHandler extends SimpleChannelInboundHandler<SDFSResponse> { // Expect Wrapper Response

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        System.out.println("[DataNode] Connected to NameNode. Sending Heartbeat...");

        // 1. Create the Inner Payload
        HeartBeatRequest heartbeat = HeartBeatRequest.newBuilder()
                .setDatanodeId(UUID.randomUUID().toString())
                .setAddress("127.0.0.1")
                .setPort(6000)
                .setFreeSpace(1024 * 1024 * 1024L)
                .build();

        // 2. Wrap it in the Envelope
        SDFSRequest request = SDFSRequest.newBuilder()
                .setHeartbeat(heartbeat) // Set the oneof field
                .setRequestId(UUID.randomUUID().toString())
                .build();

        // 3. Send the Wrapper
        ctx.writeAndFlush(request);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SDFSResponse msg) {
        // Unpack the response
        if (msg.hasHeartbeat()) {
            if (msg.getHeartbeat().getSuccess()) {
                System.out.println("[DataNode] Heartbeat Acknowledged by NameNode!");
            } else {
                System.out.println("[DataNode] Heartbeat Rejected!");
            }
        } else {
            System.out.println("[DataNode] Received unexpected response type");
        }
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}