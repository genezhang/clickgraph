# ClickGraph Examples

This directory contains examples demonstrating ClickGraph's capabilities, from simple quick starts to comprehensive real-world use cases.

## ⚡ Quick Start (`quick-start.md`)

**5-minute simple demo** - perfect for first-time users:

- **Scope**: Basic social network with 3 users and friendships
- **Data Model**: Simple users and friendships tables  
- **Setup Time**: ~5 minutes from zero to working queries
- **Queries**: Find friends, mutual connections, relationship traversals
- **Integration**: HTTP API and Neo4j driver examples

**Key Features Demonstrated**:
- ✅ Minimal data setup with Memory engine tables
- ✅ Simple YAML graph configuration 
- ✅ Basic Cypher queries for social network analysis
- ✅ Both HTTP and Bolt protocol usage
- ✅ Neo4j driver compatibility

**Perfect for**: First-time setup, learning basics, quick demos, proof-of-concept

## 📊 E-commerce Analytics (`ecommerce-analytics.md`)

**Complete end-to-end example** showcasing ClickGraph for e-commerce analytics:

- **Scope**: Customer segmentation, product recommendations, market basket analysis
- **Data Model**: Customers, products, orders, reviews with realistic relationships
- **Queries**: 15+ advanced Cypher queries for business insights
- **Integration**: HTTP API and Neo4j driver examples in Python
- **Performance**: Optimization techniques and expected benchmarks

**Key Features Demonstrated**:
- ✅ View-based graph configuration from relational tables
- ✅ Complex multi-hop traversals for recommendation systems
- ✅ Temporal analysis for seasonal purchasing patterns
- ✅ Customer journey analysis and lifetime value prediction
- ✅ Cross-selling and market basket analysis
- ✅ Robust API integration patterns

**Perfect for**: Learning ClickGraph capabilities, production implementation reference, business analytics use cases

## 🌐 Social Network View (`social_network_view.yaml`)

**Simple configuration example** for basic social network analysis:

- **Scope**: Basic social graph with users and relationships
- **Data Model**: Users table mapped to graph nodes with follows relationships
- **Usage**: Simple YAML configuration example for getting started

**Perfect for**: First-time setup, configuration reference, simple use cases

## Getting Started

1. **New to ClickGraph?** Start with the [Getting Started Guide](../docs/getting-started.md)
2. **Want comprehensive examples?** Check out [E-commerce Analytics](ecommerce-analytics.md)  
3. **Need configuration help?** See [Configuration Guide](../docs/configuration.md)
4. **API integration?** Review [API Documentation](../docs/api.md)

## Need More Examples?

We'd love to see more real-world examples! Consider contributing:

- **Financial Analytics**: Transaction networks, fraud detection, risk analysis
- **Supply Chain**: Logistics networks, supplier relationships, inventory optimization  
- **Healthcare**: Patient networks, treatment pathways, clinical trials
- **IoT/Infrastructure**: Device networks, monitoring systems, dependency mapping
- **Content/Media**: Content recommendation, social media analysis, trend detection

See our [Contributing Guidelines](../CONTRIBUTING.md) to add your own examples! 🚀

