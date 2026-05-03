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
Feature: Remove2 - Remove a Label

  Scenario: [1] Remove a single label from a node with a single label
    Given an empty graph
    And having executed:
      """
      CREATE (:L {num: 42})
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n:L
      RETURN n.num
      """
    Then the result should be, in any order:
      | n.num |
      | 42    |
    And the side effects should be:
      | -labels | 1 |

  Scenario: [2] Remove a single label from a node with two labels
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo:Bar)
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n:Foo
      RETURN labels(n)
      """
    Then the result should be, in any order:
      | labels(n) |
      | ['Bar']   |
    And the side effects should be:
      | -labels | 1 |

  Scenario: [3] Remove two labels from a node with three labels
    Given an empty graph
    And having executed:
      """
      CREATE (:L1:L2:L3 {num: 42})
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n:L1:L3
      RETURN labels(n)
      """
    Then the result should be, in any order:
      | labels(n) |
      | ['L2']    |
    And the side effects should be:
      | -labels | 2 |

  Scenario: [4] Remove a non-existent node label
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo)
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n:Bar
      RETURN labels(n)
      """
    Then the result should be, in any order:
      | labels(n) |
      | ['Foo']   |
    And no side effects

  Scenario: [5] Ignore null when removing a node label
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (a:DoesNotExist)
      REMOVE a:L
      RETURN a
      """
    Then the result should be, in any order:
      | a    |
      | null |
    And no side effects
