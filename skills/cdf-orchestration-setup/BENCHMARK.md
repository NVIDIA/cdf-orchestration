# Skill Benchmark: cdf-orchestration-setup

> ✅ **Overall verdict: PASS — Recommended for publication**

## Publication Recommendation

Recommended for publication based on the completed evaluation evidence in this report.

## Evaluation Metadata

- Skill: `cdf-orchestration-setup`
- Evaluation date: 2026-10-20
- Evaluator version: `1.5.6`
- Agents: Claude Code (`aws/anthropic/bedrock-claude-opus-4-8`), Codex (`openai/openai/gpt-5.5`)
- Tasks: 11 evaluation tasks (9 positive, 2 negative)
- Dataset digest: `sha256:4387b9f4a4d9ac65bee1d14597318ac2da37624b65c0d788a1f0689f00881a44` (skill-evaluator-dataset-snapshot/1)
- Attempts per task: 1
- Environment: `k8s-sandbox`
- Tier 2 evidence: required for publication
- Tier 3 evidence: required for publication

Each task attempt ran in its own isolated sandbox pod.

## What This Report Answers

The three-tier evaluation checks whether the skill:

- is safe to use;
- produces correct answers;
- is discovered and activated when needed;
- helps the agent complete the user's goal and expected workflow; and
- avoids wasted skill and tool usage.

## Results at a Glance

| Measure | Claude Code (Baseline → Skill Uplift) | Codex (Baseline → Skill Uplift) |
|---|---:|---:|
| Overall | 85.8% — baseline ran, but no comparable score was available; uplift unavailable | 74.9% — baseline ran, but no comparable score was available; uplift unavailable |
| Security | 90.9% → 100.0% (+9.1 points) | 68.2% → 100.0% (+31.8 points) |
| Correctness | 20.0% → 83.6% (+63.6 points) | 21.8% → 70.9% (+49.1 points) |
| Discoverability | 96.4% — baseline ran, but no comparable score was available; uplift unavailable | 58.3% — baseline ran, but no comparable score was available; uplift unavailable |
| Effectiveness | 22.4% → 58.6% (+36.2 points) | 16.7% → 52.3% (+35.6 points) |
| Efficiency | 90.3% — baseline ran, but no comparable score was available; uplift unavailable | 93.0% — baseline ran, but no comparable score was available; uplift unavailable |

**How to read this table:** baseline is the same task attempted without the target skill. Scores are rounded to one decimal; threshold-adjacent values use additional precision so their displayed band matches the verdict. Uplift is derived from those displayed scores and shown in percentage points.

Example: `47.0% → 92.0% (+45.0 points)` means the skill-assisted run scored 92.0%, 45.0 percentage points above its 47.0% no-skill baseline.

A partial dimension was calculated from only the available configured signals; review the detailed report before relying on it.

## Token Usage

Actual Tier 3 execution usage is reported for every observed agent/case pair and both conditions.

| Agent | Dataset case | With skill | Without skill | Delta | Change | Coverage |
|---|---|---:|---:|---:|---:|---|
| claude-code | All cases | 2,590,872 | 2,099,628 | +491,244 | +23.40% | skill 11/11; base 11/11 |
| claude-code | blocked-preflight-stops-setup | 66,397 | 123,607 | -57,210 | -46.28% | skill 1/1; base 1/1 |
| claude-code | dag-update-rebuilds-image | 160,618 | 89,436 | +71,182 | +79.59% | skill 1/1; base 1/1 |
| claude-code | first-install-builds-controller | 304,564 | 398,156 | -93,592 | -23.51% | skill 1/1; base 1/1 |
| claude-code | k3s-is-not-supported | 127,930 | 218,334 | -90,404 | -41.41% | skill 1/1; base 1/1 |
| claude-code | negative-prerequisite-audit | 120,359 | 90,634 | +29,725 | +32.80% | skill 1/1; base 1/1 |
| claude-code | negative-run-event-video | 30,380 | 155,818 | -125,438 | -80.50% | skill 1/1; base 1/1 |
| claude-code | one-gpu-service-mode | 274,350 | 246,946 | +27,404 | +11.10% | skill 1/1; base 1/1 |
| claude-code | osmo-external-only | 493,005 | 316,729 | +176,276 | +55.66% | skill 1/1; base 1/1 |
| claude-code | post-install-verification-retained | 368,190 | 154,265 | +213,925 | +138.67% | skill 1/1; base 1/1 |
| claude-code | preflight-required-before-install | 222,859 | 153,429 | +69,430 | +45.25% | skill 1/1; base 1/1 |
| claude-code | values-only-redeploy | 422,220 | 152,274 | +269,946 | +177.28% | skill 1/1; base 1/1 |
| codex | All cases | 1,409,131 | 3,655,359 | -2,246,228 | -61.45% | skill 11/11; base 11/11 |
| codex | blocked-preflight-stops-setup | 201,840 | 530,166 | -328,326 | -61.93% | skill 1/1; base 1/1 |
| codex | dag-update-rebuilds-image | 13,529 | 17,926 | -4,397 | -24.53% | skill 1/1; base 1/1 |
| codex | first-install-builds-controller | 144,556 | 798,473 | -653,917 | -81.90% | skill 1/1; base 1/1 |
| codex | k3s-is-not-supported | 235,511 | 115,041 | +120,470 | +104.72% | skill 1/1; base 1/1 |
| codex | negative-prerequisite-audit | 128,023 | 55,836 | +72,187 | +129.28% | skill 1/1; base 1/1 |
| codex | negative-run-event-video | 13,853 | 70,829 | -56,976 | -80.44% | skill 1/1; base 1/1 |
| codex | one-gpu-service-mode | 129,461 | 130,781 | -1,320 | -1.01% | skill 1/1; base 1/1 |
| codex | osmo-external-only | 170,494 | 1,618,745 | -1,448,251 | -89.47% | skill 1/1; base 1/1 |
| codex | post-install-verification-retained | 41,034 | 86,317 | -45,283 | -52.46% | skill 1/1; base 1/1 |
| codex | preflight-required-before-install | 152,966 | 114,879 | +38,087 | +33.15% | skill 1/1; base 1/1 |
| codex | values-only-redeploy | 177,864 | 116,366 | +61,498 | +52.85% | skill 1/1; base 1/1 |
| ALL AGENTS | Dataset aggregate | 4,000,003 | 5,754,987 | -1,754,984 | -30.50% | skill 22/22; base 22/22 |

Prompt tokens include cached reads, so total tokens are `prompt + completion` (cached is not added twice). The Efficiency score uses `(prompt - cached) + completion`. N/A means the relevant trajectory counters were not available; coverage is never estimated.

## Tier Status

| Tier | Purpose | Status | Evidence |
|---|---|---|---|
| Tier 1 | Static validation | **PASSED WITH OBSERVATIONS** | 11 validator(s); 15 finding(s) |
| Tier 2 | Semantic deduplication | **PASSED WITH OBSERVATIONS** | 2 validator(s); 4 finding(s) |
| Tier 3 | Live agent evaluation | **PASS** | 2 agent(s); 11 task(s) |

## Findings and Observations

<details>
<summary>Show detailed findings and successful checks</summary>

- **HIGH** DUPLICATE/duplicate: Duplicate content found across SKILL.md and references/deploy-controller.md:
  "## Deploy a controller on the current cluster" in SKILL.md (lines 128-131)
  vs "# Deploy the controller" in references/deploy-controller.md (lines 32-35) (`SKILL.md:128`)
- **HIGH** DUPLICATE/duplicate: Duplicate content found across SKILL.md and references/deploy-controller.md:
  "## Deploy a controller on the current cluster" in SKILL.md (lines 107-108)
  vs "# Deploy the controller" in references/deploy-controller.md (lines 19-20) (`SKILL.md:107`)
- **HIGH** DUPLICATE/duplicate: Duplicate content found across SKILL.md and references/deploy-controller.md:
  "## Deploy a controller on the current cluster" in SKILL.md (lines 116-121)
  vs "# Deploy the controller" in references/deploy-controller.md (lines 26-31) (`SKILL.md:116`)
- **HIGH** DUPLICATE/duplicate: Duplicate content found across SKILL.md and references/deploy-controller.md:
  "## Deploy a controller on the current cluster" in SKILL.md (lines 72-85)
  vs "## Deploy a controller on the current cluster" in SKILL.md (lines 87-99)
  vs "# Deploy the controller" in references/deploy-controller.md (lines 7-13) (`SKILL.md:72`)
- **MEDIUM** QUALITY/quality_efficiency: Deeply nested references in controller-connection.md (`skills/cdf-orchestration-setup/SKILL.md`)
- 14 additional finding(s) are available in the full evaluation artifacts.

</details>

## Scoring Methodology

<details>
<summary>Show dimension definitions, source signals, and thresholds</summary>

| Dimension | Question | Scored signals |
|---|---|---|
| Security | Is it safe to use? | `security` (100%) |
| Correctness | Is the answer correct? | `accuracy` (100%) |
| Discoverability | Was the right skill loaded when needed? | `skill_execution` (100%) |
| Effectiveness | Did the skill help complete the task? | `goal_accuracy` (50%) + `behavior_check` (50%) |
| Efficiency | Did it avoid wasted tool calls and token usage? | `skill_efficiency` (50%) + `token_efficiency` (50%) |

- Dimension bands: PASS at 50% or above; NEUTRAL from 40% to below 50%; FAIL below 40%.
- Overall Tier 3 lift: PASS at +5 points or more; FAIL at -10 points or less; values between those bands are NEUTRAL.
- Overall verdict: PASS only when every configured dimension passes for at least one supported agent. Lift is reported as diagnostic evidence and does not override this gate.
- The 50% attempt pass threshold is a separate per-task gate; it is not the dimension pass threshold.
- Effectiveness is the equal-weight mean of goal completion (`goal_accuracy`) and expected workflow adherence (`behavior_check`).
- Efficiency is 50% tool-call productivity (the backward-compatible `skill_efficiency` wire id) and 50% `token_efficiency`. Positive-case skill routing is scored under Discoverability, not Efficiency; a negative case without a routing target is N/A. N/A sources are omitted, remaining weights are renormalized, and the dimension is marked partial.

Signals present in this run:

- `security` (Security): unsafe operations, secret leakage, and unauthorized access.
- `skill_execution` (Skill Execution): whether the expected skill was selected, decoys were avoided, and the workflow executed.
- `skill_efficiency` (Tool Productivity): tool-call productivity (legacy wire id; routing is scored under Discoverability).
- `accuracy` (Accuracy): final-answer correctness against the reference answer.
- `goal_accuracy` (Goal Accuracy): whether the user's goal was achieved.
- `behavior_check` (Behavior Check): whether the expected workflow behavior was followed.
- `token_efficiency` (Token Efficiency): actual uncached prompt plus completion usage (50% of Efficiency).

</details>

## Freshness

Regenerate this benchmark when the skill, evaluation dataset, target agent/model, evaluator version, environment, or scoring policy changes.
