#
# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Attribution Notice under the terms of the Apache License 2.0
#
# This work was created by the collective efforts of the openCypher community.
# Without limiting the terms of Section 6, any Derivative Work that is not
# approved by the public consensus process of the openCypher Implementers Group
# should not be described as “Cypher” (and Cypher® is a registered trademark of
# Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
# proposals for change that have been documented or implemented should only be
# described as "implementation extensions to Cypher" or as "proposed changes to
# Cypher that are not yet approved by the openCypher community".
#

#encoding: utf-8

# Phase 4 import from upstream openCypher TCK (master, fetched 2026-05-02).
# File-level @wip lifted in Phase 5c. Phase 5e ungates [1] [2] [4] [6] —
# the planner now lifts `DELETE` / `SET` / `REMOVE` to peel the write off
# the root, runs the inner read pipeline through type inference (so an
# untyped `MATCH (n)` expands to a Union of typed branches), then re-wraps;
# the write_plan_builder fans out one DELETE per resolved node label via
# `find_all_alias_labels` + `slice_plan_to_label`. Of the ungated scenarios:
#   * [1] [2] — `MATCH (n) DELETE n` / `MATCH (n) DETACH DELETE n` after
#     `CREATE ()` over the universal TCK schema. The Union expansion
#     produces one DELETE per node label catalogued by `schema_gen.rs`;
#     the count probe (Phase 5d) for each per-label DELETE returns 0
#     except for `__Unlabeled` (which holds the one row), so
#     `nodes_deleted` totals to 1 across the fan-out as expected.
#   * [3] is ungated. The DETACH DELETE pipeline executes end-to-end (the
#     `MATCH (n:X) DETACH DELETE n` shape over a small `:R` fan-out; covered
#     by `cypher_detach_delete_emits_rel_then_node_delete_sequence` in
#     clickgraph-embedded). The harness asserts `-nodes` and `-relationships`
#     successfully, then hits the trailing `-labels` row — which
#     `effect_to_counter()` deliberately leaves unmapped — and records the
#     scenario as a skip (same pattern as `Create1` [3]/[4]). Re-tag
#     candidate for `@unsupported-label-mutation` once we want it filtered
#     out of the active list.
#   * [4] [6] — OPTIONAL MATCH (untyped) then DELETE / DETACH DELETE on
#     an empty graph. Multi-label fan-out emits per-label DELETEs whose
#     count probes all return 0; `no side effects` therefore holds.
#   * [5] is ungated in Phase 5d. `OPTIONAL MATCH (a:DoesNotExist) DELETE a
#     RETURN a` runs the write pipeline (no-op against an empty graph), then
#     re-runs the read pipeline with the write clauses stripped to produce
#     the user-visible `| a | null |` row. Side-effect counters are attached
#     via the new `QueryResult::get_write_counters()` side-channel; the
#     harness asserts `no side effects` against an all-zero counter map.
# Scenarios still gated with per-scenario @wip:
#   * [7] — expects a runtime ConstraintVerificationFailed; ClickGraph's
#     non-DETACH DELETE silently leaves dangling rows rather than raising
#     (engine semantics differ). Stay @wip until we add a referential-
#     integrity check or document as out-of-scope.
#   * [8] — `DELETE n:Person` (label-mutation form of DELETE) expects a
#     SyntaxError. ClickGraph rejects with a different message. Stay @wip
#     until the planner emits the openCypher diagnostic or the scenario is
#     re-tagged @unsupported-label-mutation per the same rule that applies
#     to `SET n:Label` / `REMOVE n:Label`.
Feature: Delete1 - Deleting nodes

  Scenario: [1] Delete nodes
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n)
      DELETE n
      """
    Then the result should be empty
    And the side effects should be:
      | -nodes | 1 |

  Scenario: [2] Detach delete node
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n)
      DETACH DELETE n
      """
    Then the result should be empty
    And the side effects should be:
      | -nodes | 1 |

  Scenario: [3] Detach deleting connected nodes and relationships
    Given an empty graph
    And having executed:
      """
      CREATE (x:X)
      CREATE (x)-[:R]->()
      CREATE (x)-[:R]->()
      CREATE (x)-[:R]->()
      """
    When executing query:
      """
      MATCH (n:X)
      DETACH DELETE n
      """
    Then the result should be empty
    And the side effects should be:
      | -nodes         | 1 |
      | -relationships | 3 |
      | -labels        | 1 |

  Scenario: [4] Delete on null node
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (n)
      DELETE n
      """
    Then the result should be empty
    And no side effects

  Scenario: [5] Ignore null when deleting node
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (a:DoesNotExist)
      DELETE a
      RETURN a
      """
    Then the result should be, in any order:
      | a    |
      | null |
    And no side effects

  Scenario: [6] Detach delete on null node
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (n)
      DETACH DELETE n
      """
    Then the result should be empty
    And no side effects

  @wip
  Scenario: [7] Failing when deleting connected nodes
    Given an empty graph
    And having executed:
      """
      CREATE (x:X)
      CREATE (x)-[:R]->()
      CREATE (x)-[:R]->()
      CREATE (x)-[:R]->()
      """
    When executing query:
      """
      MATCH (n:X)
      DELETE n
      """
    Then a ConstraintVerificationFailed should be raised at runtime: DeleteConnectedNode

  @wip
  Scenario: [8] Failing when deleting a label
    Given any graph
    When executing query:
      """
      MATCH (n)
      DELETE n:Person
      """
    Then a SyntaxError should be raised at compile time: InvalidDelete
