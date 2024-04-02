# Generate features

Takes the latest filtered raw data and generates both ML and non-ML features.

We'll be using MongoDB for the store instead of SQLite, for the following reasons:
- More flexible schema definitions
- JSON-optimized reads: if we stored in SQLite, we would have to store our fields in a JSON string, which requires serialization on write and deserialization on read. This can incur both time and space cost. If we store in MongoDB, we get native support for JSON/BSON which is much more efficient. We care about optimizing reads since we'll likely be doing more reads as well as reading all the data, as opposed to batch writes.
- More efficient I/O, especially for document-based queries. 

Some of the tradeoffs that we'll have to make with MongoDB are:
- Increased management cost: more to manage than a SQLite DB.
- Memory and CPU: MongoDB uses more memory and CPU to manage document storage and query processing. It also requires that the MongoDB server be kept up and running consistently.
