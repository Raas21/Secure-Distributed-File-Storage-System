S-DFS is a high-resilience, multi-node distributed file system (DFS) implemented in Java, designed to showcase advanced distributed systems and security concepts.

This project goes beyond a standard HDFS replica by enforcing a Strict WORM (Write Once, Read Many) model and integrating Client-Side Encryption (AES-256-GCM), ensuring data privacy across untrusted storage nodes.

Key Features:

Strict WORM Compliance: Prevents accidental or malicious data modification or deletion.
Client-Side Encryption: Ensures data confidentiality even if storage nodes are compromised.
Multi-Node Architecture: Supports scalable, fault-tolerant storage clusters.
Data Deduplication: Reduces storage space by eliminating duplicate files.
ABAC Access Control: Implements Attribute-Based Access Control for fine-grained authorization.