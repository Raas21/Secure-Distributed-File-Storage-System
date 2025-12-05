package com.sdfs.namenode.metadata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class INodeDirectory extends INode {
    // Map of Filename -> INode (Child)
    private final Map<String, INode> children = new ConcurrentHashMap<>();

    public INodeDirectory(String name, String owner) {
        super(name, owner);
    }

    @Override
    public boolean isDirectory() {
        return true;
    }

    public void addChild(INode node) {
        children.put(node.getName(), node);
    }

    public INode getChild(String name) {
        return children.get(name);
    }

    public boolean hasChild(String name) {
        return children.containsKey(name);
    }
}