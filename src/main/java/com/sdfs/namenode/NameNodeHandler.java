package com.sdfs.namenode;

import com.sdfs.protocol.HeartBeatRequest;
import com.sdfs.protocol.HeartBeatResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class NameNodeHandler extends SimpleChannelInboundHandler<HeartBeatRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HeartBeatRequest msg) throws Exception {
        System.out.println("[NameNode] Received HeartBeat from DataNode" + msg.getDatanodeId());
        System.out.println("[NameNode] DataNode Address: " + msg.getAddress());
        System.out.println("[NameNode] DataNode Port: " + msg.getPort());
        System.out.println("[NameNode] DataNode Free Space: " + msg.getFreeSpace());

        HeartBeatResponse response = HeartBeatResponse.newBuilder().setSuccess(true).build();

        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
