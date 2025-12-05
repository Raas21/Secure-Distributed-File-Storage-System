package com.sdfs.namenode.metadata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MetadataStore {
    // The Root of our file system ("/")
    private final INodeDirectory root;

    // Feature: Per-File/Per-Directory Locking
    // We Map a specific Path String -> A specific Lock
    private final ConcurrentHashMap<String, ReadWriteLock> pathLocks = new ConcurrentHashMap<>();

    public MetadataStore() {
        // Initialize root with owner "system"
        this.root = new INodeDirectory("/", "system");
    }

    /**
     * Helper to get or create a lock for a specific path.
     * This ensures two threads don't try to write to "/home/user/file.txt" at the
     * same time.
     */
    private ReadWriteLock getLock(String path) {
        return pathLocks.computeIfAbsent(path, k -> new ReentrantReadWriteLock());
    }

    public void acquireReadLock(String path) {
        getLock(path).readLock().lock();
    }

    public void releaseReadLock(String path) {
        getLock(path).readLock().unlock();
    }

    public void acquireWriteLock(String path) {
        getLock(path).writeLock().lock();
    }

    public void releaseWriteLock(String path) {
        getLock(path).writeLock().unlock();
    }

    /**
     * Navigates the tree to find a directory.
     * e.g., path = "/users/docs" -> Returns the INodeDirectory for "docs"
     */
    public INodeDirectory getDirectory(String path) {
        if (path.equals("/"))
            return root;

        String[] components = path.split("/");
        INodeDirectory current = root;

        for (String part : components) {
            if (part.isEmpty())
                continue; // Skip the first empty string from split

            INode child = current.getChild(part);
            if (child == null || !child.isDirectory()) {
                return null; // Path doesn't exist or is a file
            }
            current = (INodeDirectory) child;
        }
        return current;
    }

    /**
     * Creates a file in the memory tree.
     * NOTE: This does NOT write data to DataNodes yet. It just reserves the name.
     */
    public boolean createFile(String path, String filename, String owner) {
        // 1. Acquire Write Lock for the target directory
        String fullPath = path.endsWith("/") ? path + filename : path + "/" + filename;
        acquireWriteLock(fullPath);

        try {
            // 2. Find the parent directory
            INodeDirectory parent = getDirectory(path);
            if (parent == null) {
                System.err.println("Parent directory does not exist: " + path);
                return false;
            }

            // 3. Check if file already exists
            if (parent.hasChild(filename)) {
                System.err.println("File already exists: " + filename);
                return false;
            }

            // 4. Create the new Inode
            INodeFile newFile = new INodeFile(filename, owner, 0);
            parent.addChild(newFile);

            System.out.println("[MetadataStore] Created file entry: " + fullPath);
            return true;
        } finally {
            // 5. Always release the lock
            releaseWriteLock(fullPath);
        }
    }
}