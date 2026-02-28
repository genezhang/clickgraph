"""GLiNER integration for schema entity recognition."""

import logging
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Global model instance
_model: Optional["GLiNER"] = None


def get_model() -> Optional["GLiNER"]:
    """Get or initialize GLiNER model.
    
    Downloads model on first use if not available.
    """
    global _model
    
    if _model is not None:
        return _model
    
    try:
        from gliner import GLiNER
        logger.info("Loading GLiNER model...")
        # Use small model - good enough for short text like table/column names
        _model = GLiNER.from_pretrained("urchade/gliner_small-v2")
        logger.info("GLiNER model loaded successfully")
        return _model
    except Exception as e:
        logger.warning(f"Failed to load GLiNER model: {e}")
        logger.warning("Will use fallback heuristics only")
        return None


def is_model_available() -> bool:
    """Check if GLiNER model is available."""
    return get_model() is not None


def classify_table_name(table_name: str) -> tuple[str, float]:
    """Classify table name as node or edge using GLiNER.
    
    Args:
        table_name: Name of the table
        
    Returns:
        Tuple of (classification: str, confidence: float)
        classification is one of: "node", "edge", "event", "unknown"
    """
    model = get_model()
    
    if model is None:
        return "unknown", 0.0
    
    # Labels for schema classification
    labels = [
        "node entity",      # users, posts, products - noun, thing
        "relationship",     # follows, likes, orders - verb, action  
        "event",            # logs, events, actions - happening
        "dimension",        # date_dim, location_dim - lookup table
    ]
    
    try:
        # Extract base name (remove schema prefix if any)
        base_name = table_name.split(".")[-1]
        
        entities = model.predict_entities(base_name, labels)
        
        if not entities:
            return "unknown", 0.0
        
        # Get highest confidence entity
        best = max(entities, key=lambda x: x.get("score", 0))
        
        label = best.get("label", "unknown")
        score = best.get("score", 0.0)
        
        # Map to simple classification
        if "node" in label.lower():
            return "node", score
        elif "relationship" in label.lower():
            return "edge", score
        elif "event" in label.lower():
            return "event", score
        elif "dimension" in label.lower():
            return "node", score  # Dimensions are nodes
        else:
            return "unknown", score
            
    except Exception as e:
        logger.warning(f"GLiNER classification failed for '{table_name}': {e}")
        return "unknown", 0.0


def extract_entity_from_column(column_name: str) -> list[tuple[str, float]]:
    """Extract entity references from column name.
    
    For example:
        - user_id -> user
        - post_id -> post  
        - customer_sk -> customer
        - created_at -> (no entity, just attribute)
    
    Args:
        column_name: Name of the column
        
    Returns:
        List of (entity_name, confidence) tuples
    """
    model = get_model()
    
    if model is None:
        return []
    
    labels = [
        "entity reference",   # user_id, post_id
        "identifier",        # id, key
        "attribute",         # name, value, status
        "timestamp",        # created_at, updated_at
    ]
    
    try:
        entities = model.predict_entities(column_name, labels)
        
        results = []
        for entity in entities:
            text = entity.get("text", "")
            label = entity.get("label", "")
            score = entity.get("score", 0.0)
            
            # Extract entity name by removing common suffixes
            entity_name = text
            for suffix in ["_id", "_key", "_sk", "_pk"]:
                if entity_name.lower().endswith(suffix):
                    entity_name = entity_name[:-len(suffix)]
                    break
            
            # Only include entity references
            if "entity" in label.lower() or "identifier" in label.lower():
                results.append((entity_name.lower(), score))
        
        return results
        
    except Exception as e:
        logger.debug(f"Entity extraction failed for '{column_name}': {e}")
        return []


def classify_column_type(column_name: str) -> str:
    """Classify column type.
    
    Returns:
        One of: "pk", "fk", "attribute", "timestamp", "metric", "unknown"
    """
    name_lower = column_name.lower()
    
    # Primary key patterns (snake_case)
    if name_lower in ["id", "pk", "primary_key"] or name_lower.endswith("_pk"):
        return "pk"
    
    # Foreign key patterns - snake_case (_id, _key, _sk)
    if name_lower.endswith("_id") or name_lower.endswith("_key") or name_lower.endswith("_sk"):
        if name_lower.endswith("_pk"):
            return "pk"
        return "fk"
    
    # Foreign key patterns - camelCase (userId, accountId, etc.)
    # Pattern: lowercase letter or digit followed by Id or ID at end
    if re.search(r'[a-z0-9](Id|ID)$', column_name):  # e.g., userId, person1Id, userID
        return "fk"

    # Timestamp patterns
    if name_lower.endswith("_at") or "created" in name_lower or "updated" in name_lower:
        if "date" in name_lower or "time" in name_lower or name_lower.endswith("_at"):
            return "timestamp"
    
    # Metric/numeric patterns (common in fact tables)
    if any(x in name_lower for x in ["amount", "price", "qty", "quantity", "count", "sum", "total"]):
        return "metric"
    
    return "attribute"


# Fallback heuristics (used when GLiNER not available)
def classify_table_name_fallback(table_name: str) -> tuple[str, str]:
    """Fallback classification without ML.
    
    Uses simple heuristics:
    - Ends with 's' and common entity names -> node
    - Common relationship verbs -> edge
    
    Returns:
        Tuple of (classification, reason)
    """
    name = table_name.lower()
    base = name.rstrip("s")  # Remove plural
    
    # Common node tables
    node_words = [
        "user", "users", "post", "posts", "comment", "comments",
        "product", "products", "order", "orders", "customer", "customers",
        "item", "items", "category", "categories", "tag", "tags",
        "article", "articles", "message", "messages", "event", "events",
        "log", "logs", "file", "files", "image", "images",
    ]
    
    # Common edge tables
    edge_words = [
        "follow", "follows", "like", "likes", "friend", "friends",
        "subscribe", "order", "purchase", "buy", "sell",
        "relate", "link", "connect", "associate",
    ]
    
    if base in node_words:
        return "node", f"common entity: {base}"
    
    if base in edge_words:
        return "edge", f"relationship verb: {base}"
    
    # Default: check if it's a common pattern
    # _id columns suggest edge, single PK suggests node
    return "unknown", "cannot determine from name alone"
