package com.sdfs.namenode;

import com.sdfs.namenode.metadata.MetadataStore;

public class MetadataTest {
    public static void main(String[] args) {
        MetadataStore store = new MetadataStore();

        // Test 1: Create a file in Root
        System.out.println("--- Test 1: Create File in Root ---");
        boolean success1 = store.createFile("/", "test.txt", "user1");
        System.out.println("Creation Success: " + success1);

        // Test 2: Try to create duplicate (Should fail logic check)
        System.out.println("\n--- Test 2: Create Duplicate ---");
        boolean success2 = store.createFile("/", "test.txt", "user2");
        System.out.println("Creation Success: " + success2);

        // Test 3: Create file in non-existent directory (Should fail)
        System.out.println("\n--- Test 3: Invalid Directory ---");
        boolean success3 = store.createFile("/invalid/path", "doc.pdf", "user1");
        System.out.println("Creation Success: " + success3);
    }
}