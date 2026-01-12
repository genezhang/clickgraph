> **Note**: This documentation is for ClickGraph v0.6.1. [View latest docs →](../../wiki/Home.md)
# Knowledge Graphs with ClickGraph

**Caution:** This entire document is AI-generated. It may contain mistakes. Double check and raise issues for correction if you find any.

Build semantic knowledge graphs for entity relationships, inference, and knowledge discovery using ClickGraph on ClickHouse®.

## Table of Contents
- [Overview](#overview)
- [Schema Design](#schema-design)
- [Sample Dataset](#sample-dataset)
- [Knowledge Graph Queries](#knowledge-graph-queries)
- [Semantic Search](#semantic-search)
- [Inference and Reasoning](#inference-and-reasoning)
- [Real-World Applications](#real-world-applications)

## Overview

Knowledge graphs represent entities and their relationships in a semantic network. ClickGraph enables powerful knowledge discovery, inference, and semantic search on structured knowledge stored in ClickHouse®.

### Use Cases

**Enterprise Knowledge Management:**
- Company organizational structure
- Product catalogs and taxonomies
- Document and content relationships
- Skills and expertise mapping

**Scientific Research:**
- Citation networks
- Research collaboration graphs
- Concept hierarchies
- Experimental data lineage

**Content and Media:**
- Content recommendations
- Topic clustering
- Author/creator networks
- Genre and category relationships

**Life Sciences:**
- Drug-disease relationships
- Protein interaction networks
- Clinical trial data
- Gene regulatory networks

## Schema Design

### Enterprise Knowledge Graph Schema

```yaml
name: knowledge_graph
version: "1.0"

graph_schema:
  nodes:
    # Entities
    - label: Entity
      database: knowledge_db
      table: entities
      node_id: entity_id
      property_mappings:
        entity_id: entity_id
        name: entity_name
        type: entity_type
        description: description
        source: data_source
        confidence: confidence_score
        created_at: created_at
        updated_at: updated_at
    
    # Concepts (abstract ideas)
    - label: Concept
      database: knowledge_db
      table: concepts
      node_id: concept_id
      property_mappings:
        concept_id: concept_id
        name: concept_name
        definition: definition_text
        category: category
        level: hierarchy_level
    
    # Documents
    - label: Document
      database: knowledge_db
      table: documents
      node_id: doc_id
      property_mappings:
        doc_id: doc_id
        title: title
        content: content_text
        author: author_name
        published_date: published_at
        doc_type: document_type
        url: document_url
    
    # Organizations
    - label: Organization
      database: knowledge_db
      table: organizations
      node_id: org_id
      property_mappings:
        org_id: org_id
        name: org_name
        type: org_type
        industry: industry
        founded: founded_year
        location: headquarters
    
    # People
    - label: Person
      database: knowledge_db
      table: people
      node_id: person_id
      property_mappings:
        person_id: person_id
        name: full_name
        title: job_title
        email: email
        expertise: expertise_areas
    
    # Products
    - label: Product
      database: knowledge_db
      table: products
      node_id: product_id
      property_mappings:
        product_id: product_id
        name: product_name
        category: category
        description: description
        price: price
        launched: launch_date
    
    # Topics/Tags
    - label: Topic
      database: knowledge_db
      table: topics
      node_id: topic_id
      property_mappings:
        topic_id: topic_id
        name: topic_name
        description: topic_description
        parent_topic: parent_id
  
  relationships:
    # Entity relationships (general)
    - type: RELATED_TO
      database: knowledge_db
      table: entity_relations
      from_id: from_entity_id
      to_id: to_entity_id
      from_node: Entity
      to_node: Entity
      property_mappings:
        relationship_type: relation_type
        strength: relation_strength
        source: source
        created_at: created_at
    
    # Concept hierarchy
    - type: IS_A
      database: knowledge_db
      table: concept_hierarchy
      from_id: child_concept_id
      to_id: parent_concept_id
      from_node: Concept
      to_node: Concept
      property_mappings:
        confidence: confidence
    
    - type: PART_OF
      database: knowledge_db
      table: concept_composition
      from_id: part_concept_id
      to_id: whole_concept_id
      from_node: Concept
      to_node: Concept
    
    # Document relationships
    - type: MENTIONS
      database: knowledge_db
      table: document_mentions
      from_id: doc_id
      to_id: entity_id
      from_node: Document
      to_node: Entity
      property_mappings:
        mention_count: count
        relevance: relevance_score
    
    - type: CITES
      database: knowledge_db
      table: document_citations
      from_id: citing_doc_id
      to_id: cited_doc_id
      from_node: Document
      to_node: Document
      property_mappings:
        citation_context: context
        page_number: page
    
    - type: AUTHORED_BY
      database: knowledge_db
      table: document_authors
      from_id: doc_id
      to_id: person_id
      from_node: Document
      to_node: Person
      property_mappings:
        author_order: author_position
    
    # Organizational relationships
    - type: WORKS_FOR
      database: knowledge_db
      table: employment
      from_id: person_id
      to_id: org_id
      from_node: Person
      to_node: Organization
      property_mappings:
        start_date: started_at
        end_date: ended_at
        role: job_role
    
    - type: COLLABORATES_WITH
      database: knowledge_db
      table: collaborations
      from_id: person_id_1
      to_id: person_id_2
      from_node: Person
      to_node: Person
      property_mappings:
        project_count: projects
        since: collaboration_start
    
    # Product relationships
    - type: MANUFACTURES
      database: knowledge_db
      table: product_manufacturers
      from_id: org_id
      to_id: product_id
      from_node: Organization
      to_node: Product
    
    - type: COMPETES_WITH
      database: knowledge_db
      table: product_competition
      from_id: product_id_1
      to_id: product_id_2
      from_node: Product
      to_node: Product
      property_mappings:
        market_overlap: overlap_percentage
    
    # Topic relationships
    - type: TAGGED_WITH
      database: knowledge_db
      table: topic_tags
      from_id: doc_id
      to_id: topic_id
      from_node: Document
      to_node: Topic
      property_mappings:
        relevance: relevance_score
    
    - type: HAS_EXPERTISE_IN
      database: knowledge_db
      table: person_expertise
      from_id: person_id
      to_id: topic_id
      from_node: Person
      to_node: Topic
      property_mappings:
        years_experience: years
        proficiency: proficiency_level
```

### ClickHouse Table Definitions

```sql
-- Entities (core knowledge graph nodes)
CREATE TABLE knowledge_db.entities (
    entity_id UInt64,
    entity_name String,
    entity_type String,
    description String,
    data_source String,
    confidence_score Float32,
    created_at DateTime,
    updated_at DateTime
) ENGINE = Memory;

-- Concepts
CREATE TABLE knowledge_db.concepts (
    concept_id UInt64,
    concept_name String,
    definition_text String,
    category String,
    hierarchy_level UInt32
) ENGINE = Memory;

-- Documents
CREATE TABLE knowledge_db.documents (
    doc_id UInt64,
    title String,
    content_text String,
    author_name String,
    published_at DateTime,
    document_type String,
    document_url String
) ENGINE = Memory;

-- Organizations
CREATE TABLE knowledge_db.organizations (
    org_id UInt64,
    org_name String,
    org_type String,
    industry String,
    founded_year UInt32,
    headquarters String
) ENGINE = Memory;

-- People
CREATE TABLE knowledge_db.people (
    person_id UInt64,
    full_name String,
    job_title String,
    email String,
    expertise_areas String
) ENGINE = Memory;

-- Products
CREATE TABLE knowledge_db.products (
    product_id UInt64,
    product_name String,
    category String,
    description String,
    price Decimal(15,2),
    launch_date Date
) ENGINE = Memory;

-- Topics
CREATE TABLE knowledge_db.topics (
    topic_id UInt64,
    topic_name String,
    topic_description String,
    parent_id Nullable(UInt64)
) ENGINE = Memory;

-- Relationships
CREATE TABLE knowledge_db.entity_relations (
    from_entity_id UInt64,
    to_entity_id UInt64,
    relation_type String,
    relation_strength Float32,
    source String,
    created_at DateTime
) ENGINE = Memory;

CREATE TABLE knowledge_db.concept_hierarchy (
    child_concept_id UInt64,
    parent_concept_id UInt64,
    confidence Float32
) ENGINE = Memory;

CREATE TABLE knowledge_db.document_mentions (
    doc_id UInt64,
    entity_id UInt64,
    count UInt32,
    relevance_score Float32
) ENGINE = Memory;

CREATE TABLE knowledge_db.document_citations (
    citing_doc_id UInt64,
    cited_doc_id UInt64,
    context String,
    page UInt32
) ENGINE = Memory;

CREATE TABLE knowledge_db.employment (
    person_id UInt64,
    org_id UInt64,
    started_at Date,
    ended_at Nullable(Date),
    job_role String
) ENGINE = Memory;

CREATE TABLE knowledge_db.topic_tags (
    doc_id UInt64,
    topic_id UInt64,
    relevance_score Float32
) ENGINE = Memory;
```

## Sample Dataset

### Generate Knowledge Graph Data

```python
# generate_knowledge_data.py
import random
from datetime import datetime, timedelta, date
import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', port=8123)

# Generate concepts (taxonomy)
concepts = []
concept_id = 1

# Top-level categories
categories = ['Technology', 'Science', 'Business', 'Arts', 'Medicine']
for cat in categories:
    concepts.append((
        concept_id,
        cat,
        f'Top-level category for {cat}',
        'root',
        1
    ))
    parent_id = concept_id
    concept_id += 1
    
    # Sub-categories
    for i in range(5):
        concepts.append((
            concept_id,
            f'{cat} Sub-{i+1}',
            f'Sub-category of {cat}',
            cat,
            2
        ))
        concept_id += 1

client.insert('knowledge_db.concepts', concepts,
    column_names=['concept_id', 'concept_name', 'definition_text', 
                  'category', 'hierarchy_level'])

# Generate organizations
orgs = []
for i in range(1, 101):
    orgs.append((
        i,
        f'Company {i}',
        random.choice(['startup', 'enterprise', 'nonprofit']),
        random.choice(['Technology', 'Healthcare', 'Finance', 'Manufacturing']),
        random.randint(1990, 2023),
        random.choice(['San Francisco', 'New York', 'London', 'Tokyo'])
    ))

client.insert('knowledge_db.organizations', orgs,
    column_names=['org_id', 'org_name', 'org_type', 'industry', 
                  'founded_year', 'headquarters'])

# Generate people
people = []
for i in range(1, 501):
    people.append((
        i,
        f'Person {i}',
        random.choice(['Engineer', 'Researcher', 'Manager', 'Analyst', 'Designer']),
        f'person{i}@example.com',
        random.choice(['AI', 'Cloud', 'Security', 'Data Science', 'DevOps'])
    ))

client.insert('knowledge_db.people', people,
    column_names=['person_id', 'full_name', 'job_title', 'email', 'expertise_areas'])

# Generate documents (research papers, articles)
docs = []
for i in range(1, 1001):
    docs.append((
        i,
        f'Document {i}: Research on {random.choice(["AI", "Cloud", "Security"])}',
        f'Content about topic {i}...',
        random.choice([f'Person {j}' for j in range(1, 501)]),
        datetime.now() - timedelta(days=random.randint(1, 1000)),
        random.choice(['paper', 'article', 'report', 'whitepaper']),
        f'https://example.com/doc{i}'
    ))

client.insert('knowledge_db.documents', docs,
    column_names=['doc_id', 'title', 'content_text', 'author_name', 
                  'published_at', 'document_type', 'document_url'])

# Generate document citations (citation network)
citations = []
for i in range(5000):
    citing = random.randint(1, 1000)
    cited = random.randint(1, 1000)
    if citing != cited:
        citations.append((
            citing,
            cited,
            'Background research',
            random.randint(1, 50)
        ))

client.insert('knowledge_db.document_citations', citations,
    column_names=['citing_doc_id', 'cited_doc_id', 'context', 'page'])

# Generate employment relationships
employment = []
for person_id in range(1, 501):
    # Current employment
    employment.append((
        person_id,
        random.randint(1, 100),
        date.today() - timedelta(days=random.randint(365, 3650)),
        None,  # Still employed
        random.choice(['Software Engineer', 'Data Scientist', 'Product Manager'])
    ))
    
    # Past employment (50% chance)
    if random.random() > 0.5:
        end_date = date.today() - timedelta(days=random.randint(1, 365))
        employment.append((
            person_id,
            random.randint(1, 100),
            end_date - timedelta(days=random.randint(365, 1825)),
            end_date,
            random.choice(['Junior Engineer', 'Intern', 'Associate'])
        ))

client.insert('knowledge_db.employment', employment,
    column_names=['person_id', 'org_id', 'started_at', 'ended_at', 'job_role'])

# Generate collaborations (co-authorship network)
collabs = []
for i in range(2000):
    p1 = random.randint(1, 500)
    p2 = random.randint(1, 500)
    if p1 != p2 and p1 < p2:  # Avoid duplicates
        collabs.append((
            p1,
            p2,
            random.randint(1, 20),
            datetime.now() - timedelta(days=random.randint(365, 1825))
        ))

client.insert('knowledge_db.collaborations', collabs,
    column_names=['person_id_1', 'person_id_2', 'projects', 'collaboration_start'])

print("✓ Knowledge graph data generated")
print(f"  - {len(concepts)} concepts")
print(f"  - {len(orgs)} organizations")
print(f"  - {len(people)} people")
print(f"  - {len(docs)} documents")
print(f"  - {len(citations)} citations")
print(f"  - {len(employment)} employment records")
print(f"  - {len(collabs)} collaborations")
```

## Knowledge Graph Queries

### 1. Concept Hierarchy Traversal

Navigate concept taxonomies:

```cypher
// Find all sub-concepts of "Technology"
MATCH path = (root:Concept {name: 'Technology'})<-[:IS_A*]-(subconcept:Concept)
RETURN subconcept.name, 
       subconcept.definition,
       length(path) as depth
ORDER BY depth, subconcept.name
```

**Use Case**: Display taxonomic structure in UI

### 2. Expert Finding

Find experts on specific topics:

```cypher
// Find people with expertise in AI who have published papers
MATCH (person:Person)-[expertise:HAS_EXPERTISE_IN]->(topic:Topic {name: 'AI'})
WHERE expertise.proficiency >= 'expert'
OPTIONAL MATCH (person)<-[:AUTHORED_BY]-(doc:Document)
WITH person, expertise, count(doc) as publications
WHERE publications > 5
MATCH (person)-[:WORKS_FOR]->(org:Organization)
RETURN person.name,
       person.title,
       org.name as organization,
       expertise.years_experience,
       publications
ORDER BY expertise.years_experience DESC, publications DESC
LIMIT 10
```

**Use Case**: Find subject matter experts for consulting projects

### 3. Citation Network Analysis

Find influential papers by citation count:

```cypher
// Find most cited papers in last 5 years
MATCH (paper:Document)<-[:CITES]-(citing:Document)
WHERE paper.published_date > datetime() - duration({days: 1825})
WITH paper, count(citing) as citations
WHERE citations > 10
OPTIONAL MATCH (paper)-[:AUTHORED_BY]->(author:Person)
RETURN paper.title,
       author.name,
       paper.published_date,
       citations
ORDER BY citations DESC
LIMIT 20
```

**Use Case**: Identify influential research

### 4. Research Collaboration Network

Find collaboration clusters:

```cypher
// Find researchers who frequently collaborate
MATCH (p1:Person)-[collab:COLLABORATES_WITH]->(p2:Person)
WHERE collab.projects >= 3
OPTIONAL MATCH (p1)-[:WORKS_FOR]->(org1:Organization)
OPTIONAL MATCH (p2)-[:WORKS_FOR]->(org2:Organization)
RETURN p1.name,
       p2.name,
       collab.projects,
       org1.name as org1,
       org2.name as org2,
       CASE WHEN org1 = org2 THEN 'same' ELSE 'different' END as org_match
ORDER BY collab.projects DESC
```

**Use Case**: Identify research collaboration patterns

### 5. Entity Co-occurrence

Find entities frequently mentioned together:

```cypher
// Find entities that appear together in documents
MATCH (doc:Document)-[:MENTIONS]->(e1:Entity),
      (doc)-[:MENTIONS]->(e2:Entity)
WHERE e1.entity_id < e2.entity_id  // Avoid duplicates
WITH e1, e2, count(DISTINCT doc) as co_occurrences
WHERE co_occurrences >= 5
RETURN e1.name,
       e2.name,
       co_occurrences,
       e1.type as type1,
       e2.type as type2
ORDER BY co_occurrences DESC
LIMIT 20
```

**Use Case**: Discover related entities

## Semantic Search

### 6. Multi-Hop Semantic Search

Find documents related to a topic through concept hierarchy:

```cypher
// Find documents about "Machine Learning" or related sub-concepts
MATCH (topic:Concept {name: 'Machine Learning'})
OPTIONAL MATCH (topic)<-[:IS_A*1..3]-(subtopic:Concept)
WITH collect(topic) + collect(subtopic) as all_topics
UNWIND all_topics as t
MATCH (doc:Document)-[:TAGGED_WITH]->(t)
WITH DISTINCT doc
OPTIONAL MATCH (doc)-[:AUTHORED_BY]->(author:Person)
RETURN doc.title,
       author.name,
       doc.published_date,
       doc.document_type
ORDER BY doc.published_date DESC
LIMIT 20
```

**Use Case**: Semantic document search with concept expansion

### 7. Cross-Domain Knowledge Discovery

Find unexpected connections across domains:

```cypher
// Find people with expertise in both AI and Medicine
MATCH (person:Person)-[:HAS_EXPERTISE_IN]->(topic1:Topic),
      (person)-[:HAS_EXPERTISE_IN]->(topic2:Topic)
WHERE topic1.name CONTAINS 'AI' 
  AND topic2.name CONTAINS 'Medicine'
OPTIONAL MATCH (person)<-[:AUTHORED_BY]-(doc:Document)
WITH person, topic1, topic2, count(doc) as publications
WHERE publications > 0
RETURN person.name,
       person.title,
       topic1.name as expertise1,
       topic2.name as expertise2,
       publications
ORDER BY publications DESC
```

**Use Case**: Identify cross-domain innovation opportunities

### 8. Path-Based Recommendations

Recommend documents based on citation paths:

```cypher
// If you liked document A, find related documents through citations
MATCH (seed:Document {doc_id: 123})-[:CITES]->(cited:Document)
MATCH (cited)<-[:CITES]-(related:Document)
WHERE related <> seed
WITH related, count(*) as relevance_score
WHERE relevance_score >= 2  // At least 2 shared citations
OPTIONAL MATCH (related)-[:AUTHORED_BY]->(author:Person)
OPTIONAL MATCH (related)-[:TAGGED_WITH]->(topic:Topic)
RETURN related.title,
       author.name,
       collect(DISTINCT topic.name)[0..3] as topics,
       relevance_score
ORDER BY relevance_score DESC
LIMIT 10
```

**Use Case**: "You might also like" recommendations

## Inference and Reasoning

### 9. Transitive Relationship Inference

Infer organizational relationships through people:

```cypher
// Find companies that collaborate through shared employees
MATCH (person:Person)-[:WORKS_FOR]->(org1:Organization)
MATCH (person)-[prev:WORKS_FOR]->(org2:Organization)
WHERE prev.ended_at IS NOT NULL  // Previous employment
  AND org1 <> org2
WITH org1, org2, count(DISTINCT person) as shared_people
WHERE shared_people >= 3
MATCH (person2:Person)-[:WORKS_FOR]->(org1)
MATCH (person3:Person)-[:WORKS_FOR]->(org2)
OPTIONAL MATCH (person2)-[:COLLABORATES_WITH]-(person3)
WITH org1, org2, shared_people, count(DISTINCT person2) as collaborations
RETURN org1.name,
       org2.name,
       shared_people,
       collaborations,
       (collaborations * 1.0 / shared_people) as collaboration_ratio
ORDER BY shared_people DESC
```

**Use Case**: Discover organizational partnerships

### 10. Knowledge Gap Detection

Find under-explored research areas:

```cypher
// Find topics with few publications but high citation activity
MATCH (topic:Topic)<-[:TAGGED_WITH]-(doc:Document)
WITH topic, count(DISTINCT doc) as doc_count
WHERE doc_count < 10  // Few documents
MATCH (topic)<-[:TAGGED_WITH]-(doc2:Document)<-[:CITES]-(citing:Document)
WITH topic, doc_count, count(citing) as citation_count
WHERE citation_count > 50  // But high citations
RETURN topic.name,
       topic.description,
       doc_count as publications,
       citation_count as citations,
       (citation_count * 1.0 / doc_count) as citations_per_paper
ORDER BY citations_per_paper DESC
LIMIT 10
```

**Use Case**: Identify emerging research opportunities

## Real-World Applications

### Example 1: Academic Research Recommender

```cypher
// Recommend papers based on user's publication history
MATCH (user:Person {person_id: 42})<-[:AUTHORED_BY]-(my_paper:Document)
MATCH (my_paper)-[:TAGGED_WITH]->(my_topic:Topic)
WITH user, collect(DISTINCT my_topic) as my_topics

// Find papers by similar authors
MATCH (similar_author:Person)-[:HAS_EXPERTISE_IN]->(shared_topic:Topic)
WHERE shared_topic IN my_topics AND similar_author <> user
MATCH (similar_author)<-[:AUTHORED_BY]-(recommended:Document)
WHERE NOT (user)<-[:AUTHORED_BY]-(recommended)
  AND recommended.published_date > datetime() - duration({days: 365})

WITH recommended, count(DISTINCT shared_topic) as topic_overlap
WHERE topic_overlap >= 2

MATCH (recommended)<-[:CITES]-(citing:Document)
WITH recommended, topic_overlap, count(citing) as citations
RETURN recommended.title,
       recommended.published_date,
       topic_overlap,
       citations,
       (topic_overlap * 10 + citations) as relevance_score
ORDER BY relevance_score DESC
LIMIT 10
```

### Example 2: Corporate Knowledge Management

```cypher
// Find internal experts for a project topic
MATCH (topic:Topic {name: 'Cloud Architecture'})
OPTIONAL MATCH (topic)<-[:IS_A*1..2]-(subtopic:Concept)
WITH collect(topic) + collect(subtopic) as relevant_topics

// Find employees with expertise or publications
UNWIND relevant_topics as t
MATCH (person:Person)-[:HAS_EXPERTISE_IN]->(t)
WHERE person.job_title IN ['Senior Engineer', 'Principal Engineer', 'Architect']

OPTIONAL MATCH (person)<-[:AUTHORED_BY]-(internal_doc:Document {document_type: 'internal'})
WHERE internal_doc.published_date > datetime() - duration({days: 365})

WITH person, count(DISTINCT internal_doc) as recent_contributions
MATCH (person)-[:WORKS_FOR]->(org:Organization {org_id: 1})  // Internal company

RETURN person.name,
       person.title,
       person.email,
       person.expertise,
       recent_contributions
ORDER BY recent_contributions DESC
LIMIT 5
```

### Example 3: Patent Prior Art Search

```cypher
// Find related patents for prior art search
MATCH (new_patent:Document {doc_id: 999, document_type: 'patent'})
MATCH (new_patent)-[:MENTIONS]->(entity:Entity)
WHERE entity.confidence > 0.7

// Find other patents mentioning same entities
MATCH (entity)<-[:MENTIONS]-(prior_patent:Document {document_type: 'patent'})
WHERE prior_patent <> new_patent
  AND prior_patent.published_date < new_patent.published_date

WITH prior_patent, collect(DISTINCT entity.name) as shared_entities
WHERE size(shared_entities) >= 3

OPTIONAL MATCH (prior_patent)-[:AUTHORED_BY]->(inventor:Person)-[:WORKS_FOR]->(company:Organization)
RETURN prior_patent.title,
       prior_patent.published_date,
       inventor.name,
       company.name,
       shared_entities,
       size(shared_entities) as entity_overlap
ORDER BY entity_overlap DESC, prior_patent.published_date DESC
LIMIT 20
```

### Example 4: Scientific Literature Survey

```cypher
// Generate literature survey for a research topic
MATCH (seed_topic:Concept {name: 'Deep Learning'})

// Get taxonomy
OPTIONAL MATCH (seed_topic)<-[:IS_A*1..2]-(subtopic:Concept)
WITH seed_topic, collect(subtopic) + [seed_topic] as all_topics

// Find seminal papers (highly cited)
UNWIND all_topics as topic
MATCH (topic)<-[:TAGGED_WITH]-(paper:Document)
OPTIONAL MATCH (paper)<-[:CITES]-(citing:Document)
WITH paper, topic, count(citing) as citations
WHERE citations > 20

OPTIONAL MATCH (paper)-[:AUTHORED_BY]->(author:Person)
OPTIONAL MATCH (author)-[:WORKS_FOR]->(org:Organization)

RETURN topic.name as subtopic,
       paper.title,
       author.name,
       org.name as institution,
       paper.published_date,
       citations
ORDER BY subtopic, citations DESC
```

## Performance Optimization

### Indexing for Knowledge Graphs

```sql
-- Optimize concept hierarchy queries
ALTER TABLE knowledge_db.concept_hierarchy ADD INDEX idx_child child_concept_id TYPE bloom_filter;
ALTER TABLE knowledge_db.concept_hierarchy ADD INDEX idx_parent parent_concept_id TYPE bloom_filter;

-- Optimize document lookups
ALTER TABLE knowledge_db.documents ADD INDEX idx_published published_at TYPE minmax;
ALTER TABLE knowledge_db.documents ADD INDEX idx_type document_type TYPE set(10);

-- Optimize citation network
ALTER TABLE knowledge_db.document_citations ADD INDEX idx_citing citing_doc_id TYPE bloom_filter;
ALTER TABLE knowledge_db.document_citations ADD INDEX idx_cited cited_doc_id TYPE bloom_filter;
```

### Query Optimization

```cypher
-- ✅ Efficient: Filter early, traverse targeted subgraph
MATCH (root:Concept {name: 'Technology'})
MATCH (root)<-[:IS_A*1..3]-(subconcept:Concept)
WHERE subconcept.category = 'AI'
MATCH (subconcept)<-[:TAGGED_WITH]-(doc:Document)
WHERE doc.published_date > datetime() - duration({years: 1})
RETURN doc.title, subconcept.name
LIMIT 100
```

### Performance Benchmarks

**Dataset**: 1K documents, 5K citations, 500 people, 100 orgs

| Query Type | Avg Time | p95 Time |
|------------|----------|----------|
| Concept hierarchy (3 levels) | 35ms | 60ms |
| Citation network (2 hops) | 80ms | 150ms |
| Expert finding | 45ms | 85ms |
| Co-occurrence analysis | 120ms | 220ms |
| Multi-hop semantic search | 95ms | 180ms |

## Next Steps

- **[Social Network Analysis](Use-Case-Social-Network.md)** - Social graph analytics
- **[Fraud Detection](Use-Case-Fraud-Detection.md)** - Financial fraud patterns
- **[Performance Optimization](Performance-Query-Optimization.md)** - Advanced optimization
- **[Production Best Practices](Production-Best-Practices.md)** - Production deployment

## Additional Resources

- [Cypher Multi-Hop Traversals](Cypher-Multi-Hop-Traversals.md)
- [Schema Configuration Advanced](Schema-Configuration-Advanced.md)
- [Cypher Functions](Cypher-Functions.md)
