package com.sdfs.namenode.metadata;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class INode {
    protected final String name;
    protected final String owner;
    protected final long createdTime;

    // PER-FILE LOCKING:
    // Every single file/folder has its own lock.
    // Readers acquire ReadLock (shared).
    // Writers acquire WriteLock (exclusive).
    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public INode(String name, String owner) {
        this.name = name;
        this.owner = owner;
        this.createdTime = System.currentTimeMillis();
    }

    public String getName() {
        return name;
    }

    public String getOwner() {
        return owner;
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    public void writeLock() {
        lock.writeLock().lock();
    }

    public void writeUnlock() {
        lock.writeLock().unlock();
    }

    public abstract boolean isDirectory();
}