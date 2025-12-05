package com.sdfs.namenode.metadata;

public class INodeFile extends INode {
    private long size;

    public INodeFile(String name, String owner, long size) {
        super(name, owner);
        this.size = size;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    public long getSize() {
        return size;
    }
}