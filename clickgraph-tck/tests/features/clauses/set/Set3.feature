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
Feature: Set3 - Set a Label

  Scenario: [1] Add a single label to a node with no label
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n)
      SET n:Foo
      RETURN n
      """
    Then the result should be, in any order:
      | n      |
      | (:Foo) |
    And the side effects should be:
      | +labels | 1 |

  Scenario: [2] Adding multiple labels to a node with no label
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n)
      SET n:Foo:Bar
      RETURN n
      """
    Then the result should be, in any order:
      | n          |
      | (:Foo:Bar) |
    And the side effects should be:
      | +labels | 2 |

  Scenario: [3] Add a single label to a node with an existing label
    Given an empty graph
    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      MATCH (n:A)
      SET n:Foo
      RETURN n
      """
    Then the result should be, in any order:
      | n        |
      | (:A:Foo) |
    And the side effects should be:
      | +labels | 1 |

  Scenario: [4] Adding multiple labels to a node with an existing label
    Given an empty graph
    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      MATCH (n)
      SET n:Foo:Bar
      RETURN n
      """
    Then the result should be, in any order:
      | n            |
      | (:A:Foo:Bar) |
    And the side effects should be:
      | +labels | 2 |

  Scenario: [5] Ignore whitespace before colon 1
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n)
      SET n :Foo
      RETURN labels(n)
      """
    Then the result should be, in any order:
      | labels(n) |
      | ['Foo']   |
    And the side effects should be:
      | +labels | 1 |

  Scenario: [6] Ignore whitespace before colon 2
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n)
      SET n :Foo :Bar
      RETURN labels(n)
      """
    Then the result should be (ignoring element order for lists):
      | labels(n)      |
      | ['Foo', 'Bar'] |
    And the side effects should be:
      | +labels | 2 |

  Scenario: [7] Ignore whitespace before colon 3
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (n)
      SET n :Foo:Bar
      RETURN labels(n)
      """
    Then the result should be (ignoring element order for lists):
      | labels(n)      |
      | ['Foo', 'Bar'] |
    And the side effects should be:
      | +labels | 2 |

  Scenario: [8] Ignore null when setting label
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (a:DoesNotExist)
      SET a:L
      RETURN a
      """
    Then the result should be, in any order:
      | a    |
      | null |
    And no side effects
