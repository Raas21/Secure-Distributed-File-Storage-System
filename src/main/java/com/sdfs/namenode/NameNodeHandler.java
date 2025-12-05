package com.sdfs.namenode;

import com.sdfs.namenode.metadata.MetadataStore;
import com.sdfs.protocol.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class NameNodeHandler extends SimpleChannelInboundHandler<SDFSRequest> {

    private final MetadataStore metadataStore;

    public NameNodeHandler(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SDFSRequest msg) throws Exception {
        SDFSResponse.Builder responseWrapper = SDFSResponse.newBuilder();

        // ROUTING LOGIC
        switch (msg.getPayloadCase()) {
            case HEARTBEAT:
                handleHeartbeat(msg.getHeartbeat(), responseWrapper);
                break;
            case CREATE_FILE:
                handleCreateFile(msg.getCreateFile(), responseWrapper);
                break;
            default:
                System.out.println("[NameNode] Unknown Request Type Received");
        }

        ctx.writeAndFlush(responseWrapper.build());
    }

    private void handleHeartbeat(HeartBeatRequest req, SDFSResponse.Builder responseWrapper) {
        System.out.println("[NameNode] HeartBeat from: " + req.getDatanodeId());

        HeartBeatResponse hbResponse = HeartBeatResponse.newBuilder()
                .setSuccess(true)
                .build();

        responseWrapper.setHeartbeat(hbResponse);
    }

    private void handleCreateFile(CreateFileRequest req, SDFSResponse.Builder responseWrapper) {
        System.out.println("[NameNode] CreateFile Request: " + req.getPath() + "/" + req.getFilename());

        // 1. Call the Metadata Brain
        boolean isCreated = metadataStore.createFile(req.getPath(), req.getFilename(), req.getOwnerId());

        // 2. Construct Response
        CreateFileResponse.Builder cfResponse = CreateFileResponse.newBuilder()
                .setSuccess(isCreated);

        if (!isCreated) {
            cfResponse.setErrorMessage("File already exists or parent path missing.");
        } else {
            // In Phase 2, we will allocate blocks here.
            // For now, return empty block location or success message.
            cfResponse.setFileId("FILE-" + System.currentTimeMillis());
        }

        responseWrapper.setCreateFile(cfResponse);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}