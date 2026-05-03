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

# @wip — Phase 4 import from upstream openCypher TCK (master, fetched 2026-05-02).
# Cypher write clauses (CREATE / SET / DELETE / REMOVE) are implemented in
# embedded mode as of v0.6.7, but this scenario file requires harness
# extensions before it runs cleanly:
#   * `the side effects should be:` step currently no-ops; needs counter
#     assertion against the QueryResult counters returned by handle_write_async.
#   * Some scenarios use anonymous nodes (`CREATE ()`) that schema_gen.rs
#     doesn't yet collect into the universal catalog.
#   * Several scenarios chain CREATE/MATCH/SET in patterns that exercise
#     unimplemented combinations (CREATE … RETURN, relationship CREATE,
#     map-merge SET) — see KNOWN_ISSUES.md.
# Lifted incrementally as triage lands. See docs/design/embedded-writes.md
# Appendix (Phase 4) for the plan.
@wip
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

  Scenario: [8] Failing when deleting a label
    Given any graph
    When executing query:
      """
      MATCH (n)
      DELETE n:Person
      """
    Then a SyntaxError should be raised at compile time: InvalidDelete
