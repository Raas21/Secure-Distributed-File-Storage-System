package com.sdfs.datanode;

import com.sdfs.protocol.HeartBeatRequest;
import com.sdfs.protocol.HeartBeatResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.UUID;

public class DataNodeHandler extends SimpleChannelInboundHandler<HeartBeatResponse> {

    // This runs automatically when the connection is established
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        System.out.println("[DataNode] Connected to NameNode. Sending Heartbeat...");

        // Create a fake Heartbeat
        HeartBeatRequest heartbeat = HeartBeatRequest.newBuilder()
                .setDatanodeId(UUID.randomUUID().toString())
                .setAddress("127.0.0.1")
                .setPort(6000)
                .setFreeSpace(1024 * 1024 * 1024L) // 1GB fake space
                .build();

        // Send it
        ctx.writeAndFlush(heartbeat);
    }

    // This runs when the NameNode responds
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HeartBeatResponse msg) {
        if (msg.getSuccess()) {
            System.out.println("[DataNode] Heartbeat Acknowledged by NameNode!");
        } else {
            System.out.println("[DataNode] Heartbeat Rejected!");
        }
        // For this test, we close connection after one ping
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}