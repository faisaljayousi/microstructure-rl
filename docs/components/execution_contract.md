# Execution Contract

This document specifies the execution semantics of the market simulator. It is intended to be read as a *formal contract* between historical market data, the simulator, and any agent interacting with it.

<!-- vscode-markdown-toc -->
* 1. [Purpose](#Purpose)
* 2. [Data Model & Hard Constraints](#DataModelHardConstraints)
	* 2.1. [Input Market Data](#InputMarketData)
	* 2.2. [Observability Limits](#ObservabilityLimits)
* 3. [Causality and Look-Ahead Elimination](#CausalityandLook-AheadElimination)
	* 3.1. [ Step Scheduling (Operational Causality)](#StepSchedulingOperationalCausality)
* 4. [Order Types Supported](#OrderTypesSupported)
* 5. [Order Lifecycle](#OrderLifecycle)
* 6. [Queueing & Priority Model](#QueueingPriorityModel)
	* 6.1. [Price–Time Priority (Approximate)](#PriceTimePriorityApproximate)
	* 6.2. [Queue Depletion Inference](#QueueDepletionInference)
* 7. [Fill Rules](#FillRules)
	* 7.1. [Cross-Through Fills (Aggressive)](#Cross-ThroughFillsAggressive)
		* 7.1.1. [Execution Price and Sweep Semantics](#ExecutionPriceandSweepSemantics)
		* 7.1.2. [Fill Event Logging for Sweeps (Slippage Observability)](#FillEventLoggingforSweepsSlippageObservability)
	* 7.2. [At-Touch Passive Fills](#At-TouchPassiveFills)
		* 7.2.1. [Effective Depletion Minimum](#EffectiveDepletionMinimum)
		* 7.2.2. [Deterministic Allocation at a Price Level](#DeterministicAllocationataPriceLevel)
	* 7.3. [Vanishing Liquidity Rule](#VanishingLiquidityRule)
* 8. [Out-of-Band (Deep Book) Orders](#Out-of-BandDeepBookOrders)
* 9. [Self-Trade Prevention (STP)](#Self-TradePreventionSTP)
* 10. [Accounting & Fees](#AccountingFees)
	* 10.1. [Fixed-Point Accounting](#Fixed-PointAccounting)
		* 10.1.1. [Notional and Normalisation](#NotionalandNormalisation)
	* 10.2. [Fees](#Fees)
		* 10.2.1. [Fee Computation and Attribution](#FeeComputationandAttribution)
* 11. [Determinism & Reproducibility](#DeterminismReproducibility)

<!-- vscode-markdown-toc-config
	numbering=true
	autoSave=true
	/vscode-markdown-toc-config -->
<!-- /vscode-markdown-toc -->

---

##  1. <a name='Purpose'></a>Purpose

The simulator replays historical L2 snapshot market data (`.snap` records) and executes an agent’s orders under an explicit, auditable model of latency, queuing and priority, fills and partial fills, as well as fees and accounting.

---

##  2. <a name='DataModelHardConstraints'></a>Data Model & Hard Constraints

###  2.1. <a name='InputMarketData'></a>Input Market Data

The simulator consumes a sequence of `Record` snapshots containing top-N (N=20) bid and ask levels. Each record provides:
- `ts_recv_ns`: local receive timestamp (monotone, simulator clock)
- `ts_event_ms`: exchange timestamp (may be missing or zero)
- price and displayed quantity per level

In the event of multiple records sharing the same `ts_event_ms`, the simulator processes them sequentially in `ts_recv_ns` order.

###  2.2. <a name='ObservabilityLimits'></a>Observability Limits

Given snapshot-only L2 data:

- Individual trades and cancels are not observable
- True FIFO queue position is not observable
- Queue depletion can only be inferred from changes in displayed quantity

---

##  3. <a name='CausalityandLook-AheadElimination'></a>Causality and Look-Ahead Elimination

The simulator enforces strict causal ordering:

- Agent actions cannot result in fills at the same observed market timestamp.
- All orders become active only after a modelled outbound latency.
- Market observations presented to the agent may be delayed by an observation latency.

###  3.1. <a name='StepSchedulingOperationalCausality'></a> Step Scheduling (Operational Causality)

To make the "no same-tick fills" rule mechanically enforceable, each `step(snapshot t)` is ordered as:

1. **Queue update**: update `qty_ahead` / visibility state using the transition from snapshot `t-1` to `t`.
2. **Matching & fills**: evaluate fills only for orders that were `ACTIVE` strictly before snapshot `t` (i.e., became active during a prior `step()` call).
3. **Activation**: pop newly due orders from the outbound-latency heap and transition them from `PENDING` to `ACTIVE`.

Newly activated orders are not fill-eligible until the next `step()`.

---

##  4. <a name='OrderTypesSupported'></a>Order Types Supported 

- Limit orders
- Cancel requests
- Market orders (modelled as aggressive sweeps of visible depth)

Complex order types (iceberg, pegged, hidden) are out of scope.

---

##  5. <a name='OrderLifecycle'></a>Order Lifecycle

| State           | Description                                                      | Transition Trigger                                                                             | Simulator Impact                                                                                                                        |
| :-------------- | :--------------------------------------------------------------- | :--------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------- |
| **`PENDING`**   | Order accepted by the simulator but not yet live on the exchange | Agent calls `place_limit()` / `place_market()` and passes validation                           | Capital is locked immediately. Order is enqueued in the latency heap and is invisible to the market and to STP checks.              |
| **`ACTIVE`**    | Order is live on the simulated exchange                          | `current_ts ≥ activate_ts` during `step()`                                                     | Order becomes visible to the simulator. It joins the tail of the queue at its price level (if visible) and is subject to STP rules. |
| **`PARTIAL`**   | Order has been partially executed                                | Partial fill occurs                                                          | Filled quantity is booked; remaining quantity retains queue position.                                                                   |
| **`FILLED`**    | Order is fully executed                                          | `filled_qty == original_qty`                                                 | Order is terminal. Remaining locks are released. Final PnL and fees are accounted.                                                      |
| **`CANCELLED`** | Order explicitly cancelled or cancelled by STP                   | Agent calls `cancel()` **or** STP policy cancels resting orders                                | Remaining locked capital is released. Queue position is lost permanently.                                                               |
| **`REJECTED`**  | Order never enters the simulator                                 | Validation failure at submission **or** STP rejection at activation **or** capacity exhaustion | No market impact. Any provisional locks are rolled back. Deterministic error is returned to the agent.                                  |


State transitions are timestamped using the simulator clock (`ts_recv_ns`).

The simulator enforces hard limits on internal resources (maximum open orders, events). Exceeding these limits results in deterministic order rejection with `InsufficientResources`.

---

##  6. <a name='QueueingPriorityModel'></a>Queueing & Priority Model

###  6.1. <a name='PriceTimePriorityApproximate'></a>Price–Time Priority (Approximate)

The simulator enforces price priority exactly.

Time priority at a given price is approximated as follows:
- When an order becomes active, it is assumed to join the back of the displayed queue at that price.
- The simulator tracks a per-order `qty_ahead` (in fixed-point quantity units), representing displayed liquidity ahead of the agent.

###  6.2. <a name='QueueDepletionInference'></a>Queue Depletion Inference

Let:
- `q_prev` = displayed quantity at price `p` in snapshot `t`
- `q_next` = displayed quantity at price `p` in snapshot `t+1`

Then:

    depletion = max(0, q_prev - q_next)

Only a fraction of this depletion is attributed to trades: effective_depletion = $\alpha$ * depletion, $\alpha \in [0, 1]$.

This models the ambiguity between trades and cancels. While $\alpha$ is a global constant in this version, it serves as a conservative floor for fill probability. A value of $\alpha=1.0$ assumes a 'Perfect Information' environment where all quantity changes are trades.

---

##  7. <a name='FillRules'></a>Fill Rules 

###  7.1. <a name='Cross-ThroughFillsAggressive'></a>Cross-Through Fills (Aggressive)

An order is filled immediately if:
- Buy order: best ask ≤ limit price
- Sell order: best bid ≥ limit price

Execution price is defined explicitly (e.g. best opposing price at match time).

####  7.1.1. <a name='ExecutionPriceandSweepSemantics'></a>Execution Price and Sweep Semantics

When an order is marketable, it executes as an aggressive sweep of visible opposing depth (top-N levels only).

- Limit (marketable) orders sweep level-by-level, best price outward, until:
  1) the order is fully filled, or
  2) the next opposing level price violates the limit, or
  3) visible depth is exhausted.

  Each partial fill is priced at the consumed level's displayed price.


####  7.1.2. <a name='FillEventLoggingforSweepsSlippageObservability'></a>Fill Event Logging for Sweeps (Slippage Observability)

Aggressive sweeps are decomposed into discrete events per price level to provide high-fidelity feedback for RL agent training.

* If a sweep consumes multiple book levels, the simulator generates one FillEvent for each unique price/quantity pair hit. 
* While a Weighted Average Price (WAP) may be calculated for reporting, the raw per-level events remain the primary source of truth for the ledger.

This logging requirement allows for precise calculation of Realised Slippage (the difference between the best-observed price and the actual execution price across the sweep).
 


###  7.2. <a name='At-TouchPassiveFills'></a>At-Touch Passive Fills

A passive fill at price `p` is allowed only if:
1. The price level `p` remains visible in top-N
2. Displayed quantity at `p` decreases
3. The agent’s `qty_ahead` is fully depleted

Fills occur only after `qty_ahead` reaches zero and only up to the remaining effective depletion.

####  7.2.1. <a name='EffectiveDepletionMinimum'></a>Effective Depletion Minimum

To prevent an agent's queue position from stalling indefinitely, the simulator enforces a minimum depletion floor. If any liquidity is removed from a price level ($\text{depletion}\_q > 0$), the effective_depletion is guaranteed to be at least 1 unit. This ensures that even in low-volume or low-$\alpha$ regimes, integer truncation cannot result in "immortal" queue positions.

####  7.2.2. <a name='DeterministicAllocationataPriceLevel'></a>Deterministic Allocation at a Price Level

Let `E_p` be the effective depletion available at price `p` in the current step. Passive fills at price `p` are allocated deterministically across eligible agent orders at `p`:

1. Eligible orders are processed in activation order (approx FIFO), tie-broken by increasing `order_id`.
2. For each order in that order:
   - `fill_qty = min(remaining_qty, E_p_remaining)`
   - decrement `E_p_remaining` by `fill_qty`
3. Stop when `E_p_remaining == 0`.

All passive fills are classified as **MAKER** fills for fee purposes.

###  7.3. <a name='VanishingLiquidityRule'></a>Vanishing Liquidity Rule

If a price level disappears from the top-N depth:
- Queue tracking at that price is frozen
- Passive fills at that level are disallowed
- Only cross-through fills remain possible

This avoids attributing fills to unobservable deep-book behaviour.

---

##  8. <a name='Out-of-BandDeepBookOrders'></a>Out-of-Band (Deep Book) Orders

Orders priced outside the visible top-N depth are considered blind:
- No queue position or depletion can be inferred
- No passive fills are allowed while the price is not visible
- Orders may still fill via aggressive cross-through if the market moves sufficiently

---

##  9. <a name='Self-TradePreventionSTP'></a>Self-Trade Prevention (STP)

The simulator prevents an agent from trading against their own resting limit orders. The behaviour is governed by a configurable `StpPolicy` applied at the moment an order attempts to transition from `PENDING` to `ACTIVE`:

| Policy | Behaviour |
| :--- | :--- |
| **`RejectIncoming`** | If the activating order crosses a resting order, the incoming order is marked as `REJECTED`. |
| **`CancelResting`** | All resting orders that would cross the incoming order are transitioned to `CANCELLED`, then the incoming order becomes `ACTIVE`. |
| **`None`** | No checks are performed; self-trades are allowed to execute. |

STP checks are deterministic and respect the modelled outbound latency. An order in the `PENDING` state is "invisible" to the matching engine and therefore cannot trigger STP until its activation timestamp is reached.

---

##  10. <a name='AccountingFees'></a>Accounting & Fees

###  10.1. <a name='Fixed-PointAccounting'></a>Fixed-Point Accounting

- Prices, quantities, cash, and inventory are maintained in fixed-point `int64`
- Floating point is used only for reporting and reward computation

####  10.1.1. <a name='NotionalandNormalisation'></a>Notional and Normalisation

For a fill of `qty_q` at `price_q`, the cash notional in `cash_q` units is:

    notional_cash_q = floor((price_q * qty_q) / PRICE_SCALE)

All intermediate products MUST use 128-bit integer arithmetic to avoid overflow.

###  10.2. <a name='Fees'></a>Fees

- Maker and taker fees are explicitly defined
- Fees are applied immediately upon fill
- All PnL and rewards are net of fees

Fee schedules are configurable but fixed for a given run.

####  10.2.1. <a name='FeeComputationandAttribution'></a>Fee Computation and Attribution

For a fill with `notional_cash_q`:

    fee_cash_q = floor((notional_cash_q * fee_ppm) / PPM_SCALE)

Fee attribution is determined by the fill mechanism:
- **TAKER**: any fill produced by aggressive sweep matching (Cross-Through fills, including market orders).
- **MAKER**: any fill produced by passive at-touch queue depletion.

Fees are applied atomically with the fill and immediately reflected in available balances.



---

##  11. <a name='DeterminismReproducibility'></a>Determinism & Reproducibility

Given:
- identical market data
- identical parameters (latency, alpha, fees)
- identical agent actions

The simulator produces bitwise-identical execution outcomes.
