# Motivation and Rationale
- Viewing ClickHouse databases (including external sources) as graph data with graph analytics capability brings another level of abstraction and boosts productivity with graph tools, and enables agentic GraphRAG support with local writes.
- Research shows relational analytics with columnar stores and vectorized execution engines like ClickHouse provide superior analytical performance and scalability to graph-native technologies, which usually leverage explicit adjacency representations and are more suitable for local-area graph traversals.
- View-based graph analytics offer the benefits of zero-ETL without the hassle of data migration and duplicate cost, yet better performance and scalability than most of the native graph analytics options.
- Neo4j Bolt protocol support gives access to the tools available based on the Bolt protocol.
- Embedded mode and CLI support agentic workflows better.
