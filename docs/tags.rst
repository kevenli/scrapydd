Tags
====

For some reason (e.g. specify an only ip for one spider in a large cluster), we need an agent and spider matching
mechanism. The tags system is here for that.

Spider may has up to one tag. Agent may has many tags. A spider must be run on the agent which has its tag.

Matching rules see:

+------------------------+----------+-----------+-----------+-------------+
| Spider Tag, Agent Tags | None     | a         | b         | a, b        |
+------------------------+----------+-----------+-----------+-------------+
| None                   | True     | False     | False     | False       |
+------------------------+----------+-----------+-----------+-------------+
| a                      | False    | True      | False     | True        |
+------------------------+----------+-----------+-----------+-------------+
| b                      | False    | False     | True      | True        |
+------------------------+----------+-----------+-----------+-------------+

Tips:
An agent with some tag will never match any none tag spider.
