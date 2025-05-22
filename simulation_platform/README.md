# Simulation Platform

## Current design (V1)

```
                    +------------------------+
                    |   SimulationManager    |
                    |------------------------|
                    | - config               |
                    | - run_simulation()     |
                    +-----------+------------+
                                |
                                v
                    +------------------------+
                    |   Agent                |
                    |------------------------|
                    | - profile              |
                    | - state                |
                    | - history              |
                    | - simulate_round()     |
                    +------------------------+
                                |
                                v
+----------------+   +---------------------+   +-------------------+
| AgentProfile   |-->| AgentBeliefs        |<--| BeliefUpdater     |
|                |   | - political_views   |   | - update()        |
|                |   | - worldview         |   +-------------------+
+----------------+   +---------------------+

+----------------+   +---------------------+   +-------------------+
| AgentState     |<--| FeedEngine          |<--| FeedRankingModel  |
|                |   +---------------------+   +-------------------+
+----------------+           |
                             v
                     +----------------------+
                     | PostContent          |
                     | (authored/generated) |
                     +----------------------+
```
