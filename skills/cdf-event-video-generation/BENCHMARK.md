# Skill Benchmark: cdf-event-video-generation

> ✅ **Overall verdict: PASS — Recommended for publication**

## Publication Recommendation

Recommended for publication based on the completed evaluation evidence in this report.

## Evaluation Metadata

- Skill: `cdf-event-video-generation`
- Evaluation date: 2026-10-20
- Evaluator version: `1.5.6`
- Agents: Claude Code (`aws/anthropic/bedrock-claude-opus-4-8`), Codex (`openai/openai/gpt-5.5`)
- Tasks: 15 evaluation tasks (15 positive)
- Dataset digest: `sha256:777897553a6f3d3a1d11c7a4c5e3dfebbcb28e750b5631f1caf902969d0f7cb3` (skill-evaluator-dataset-snapshot/1)
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
| Overall | 89.3% — baseline ran, but no comparable score was available; uplift unavailable | 87.6% — baseline ran, but no comparable score was available; uplift unavailable |
| Security | 66.7% → 100.0% (+33.3 points) | 50.0% → 93.3% (+43.3 points) |
| Correctness | 40.0% → 98.7% (+58.7 points) | 32.0% → 96.0% (+64.0 points) |
| Discoverability | 78.9% — baseline ran, but no comparable score was available; uplift unavailable | 78.0% — baseline ran, but no comparable score was available; uplift unavailable |
| Effectiveness | 45.9% → 81.1% (+35.2 points) | 36.8% → 80.2% (+43.4 points) |
| Efficiency | 87.9% — baseline ran, but no comparable score was available; uplift unavailable | 90.3% — baseline ran, but no comparable score was available; uplift unavailable |

**How to read this table:** baseline is the same task attempted without the target skill. Scores are rounded to one decimal; threshold-adjacent values use additional precision so their displayed band matches the verdict. Uplift is derived from those displayed scores and shown in percentage points.

Example: `47.0% → 92.0% (+45.0 points)` means the skill-assisted run scored 92.0%, 45.0 percentage points above its 47.0% no-skill baseline.

A partial dimension was calculated from only the available configured signals; review the detailed report before relying on it.

## Token Usage

Actual Tier 3 execution usage is reported for every observed agent/case pair and both conditions.

| Agent | Dataset case | With skill | Without skill | Delta | Change | Coverage |
|---|---|---:|---:|---:|---:|---|
| claude-code | All cases | 3,094,471 | 11,611,348 | -8,516,877 | -73.35% | skill 15/15; base 15/15 |
| claude-code | evg-anomaly-distribution-positive | 161,893 | 255,283 | -93,390 | -36.58% | skill 1/1; base 1/1 |
| claude-code | evg-defect-routing-negative | 295,477 | 1,669,997 | -1,374,520 | -82.31% | skill 1/1; base 1/1 |
| claude-code | evg-e2e-external-positive | 247,868 | 260,479 | -12,611 | -4.84% | skill 1/1; base 1/1 |
| claude-code | evg-e2e-internal-positive | 379,051 | 4,763,354 | -4,384,303 | -92.04% | skill 1/1; base 1/1 |
| claude-code | evg-extra-distribution-key-negative | 115,880 | 250,850 | -134,970 | -53.81% | skill 1/1; base 1/1 |
| claude-code | evg-generation-only-unsupported | 70,724 | 214,974 | -144,250 | -67.10% | skill 1/1; base 1/1 |
| claude-code | evg-local-upload-positive | 251,448 | 183,666 | +67,782 | +36.91% | skill 1/1; base 1/1 |
| claude-code | evg-missing-output-clarification | 157,985 | 157,949 | +36 | +0.02% | skill 1/1; base 1/1 |
| claude-code | evg-no-dataset-negative | 113,437 | 311,843 | -198,406 | -63.62% | skill 1/1; base 1/1 |
| claude-code | evg-no-secret-leak-security | 29,645 | 29,491 | +154 | +0.52% | skill 1/1; base 1/1 |
| claude-code | evg-not-iaa-attribute-search-routing-negative | 217,074 | 1,618,232 | -1,401,158 | -86.59% | skill 1/1; base 1/1 |
| claude-code | evg-not-iaa-clothing-routing-negative | 123,844 | 1,237,894 | -1,114,050 | -90.00% | skill 1/1; base 1/1 |
| claude-code | evg-platform-selection-positive | 70,775 | 90,684 | -19,909 | -21.95% | skill 1/1; base 1/1 |
| claude-code | evg-status-results-positive | 427,428 | 350,669 | +76,759 | +21.89% | skill 1/1; base 1/1 |
| claude-code | evg-vda-routing-negative | 431,942 | 215,983 | +215,959 | +99.99% | skill 1/1; base 1/1 |
| codex | All cases | 2,489,771 | 3,564,871 | -1,075,100 | -30.16% | skill 15/15; base 15/15 |
| codex | evg-anomaly-distribution-positive | 49,409 | 13,819 | +35,590 | +257.54% | skill 1/1; base 1/1 |
| codex | evg-defect-routing-negative | 310,422 | 437,843 | -127,421 | -29.10% | skill 1/1; base 1/1 |
| codex | evg-e2e-external-positive | 187,959 | 211,034 | -23,075 | -10.93% | skill 1/1; base 1/1 |
| codex | evg-e2e-internal-positive | 118,489 | 551,408 | -432,919 | -78.51% | skill 1/1; base 1/1 |
| codex | evg-extra-distribution-key-negative | 137,003 | 114,310 | +22,693 | +19.85% | skill 1/1; base 1/1 |
| codex | evg-generation-only-unsupported | 145,738 | 98,382 | +47,356 | +48.13% | skill 1/1; base 1/1 |
| codex | evg-local-upload-positive | 151,598 | 130,685 | +20,913 | +16.00% | skill 1/1; base 1/1 |
| codex | evg-missing-output-clarification | 69,798 | 360,082 | -290,284 | -80.62% | skill 1/1; base 1/1 |
| codex | evg-no-dataset-negative | 98,042 | 523,237 | -425,195 | -81.26% | skill 1/1; base 1/1 |
| codex | evg-no-secret-leak-security | 30,282 | 13,381 | +16,901 | +126.31% | skill 1/1; base 1/1 |
| codex | evg-not-iaa-attribute-search-routing-negative | 141,635 | 390,090 | -248,455 | -63.69% | skill 1/1; base 1/1 |
| codex | evg-not-iaa-clothing-routing-negative | 169,161 | 136,438 | +32,723 | +23.98% | skill 1/1; base 1/1 |
| codex | evg-platform-selection-positive | 69,263 | 68,999 | +264 | +0.38% | skill 1/1; base 1/1 |
| codex | evg-status-results-positive | 336,731 | 444,496 | -107,765 | -24.24% | skill 1/1; base 1/1 |
| codex | evg-vda-routing-negative | 474,241 | 70,667 | +403,574 | +571.09% | skill 1/1; base 1/1 |
| ALL AGENTS | Dataset aggregate | 5,584,242 | 15,176,219 | -9,591,977 | -63.20% | skill 30/30; base 30/30 |

Prompt tokens include cached reads, so total tokens are `prompt + completion` (cached is not added twice). The Efficiency score uses `(prompt - cached) + completion`. N/A means the relevant trajectory counters were not available; coverage is never estimated.

## Tier Status

| Tier | Purpose | Status | Evidence |
|---|---|---|---|
| Tier 1 | Static validation | **PASSED WITH OBSERVATIONS** | 11 validator(s); 31 finding(s) |
| Tier 2 | Semantic deduplication | **PASSED WITH OBSERVATIONS** | 2 validator(s); 1 finding(s) |
| Tier 3 | Live agent evaluation | **PASS** | 2 agent(s); 15 task(s) |

## Findings and Observations

<details>
<summary>Show detailed findings and successful checks</summary>

- **HIGH** DUPLICATE/duplicate: Duplicate content found across SKILL.md and references/setup-and-preflight.md:
  "## Scope" in SKILL.md (lines 197-225)
  vs "## GPU requirements" in references/setup-and-preflight.md (lines 83-110) (`SKILL.md:197`)
- **MEDIUM** QUALITY/quality_correctness: No documented scripts in table format (`skills/cdf-event-video-generation/SKILL.md`)
- **MEDIUM** QUALITY/quality_correctness: Instructions don't mention 'run_script' (`skills/cdf-event-video-generation/SKILL.md`)
- **MEDIUM** QUALITY/quality_efficiency: Large skill (7421 tokens, recommended max <5000). Per agentskills.io, SKILL.md should be concise (~500 lines) — large skill bodies increase token cost after invocation; long or unfocused top-level descriptions can degrade agent routing accuracy (`skills/cdf-event-video-generation/SKILL.md`)
- **MEDIUM** QUALITY/quality_efficiency: Deeply nested references in airflow-direct-api.md (`skills/cdf-event-video-generation/SKILL.md`)
- 27 additional finding(s) are available in the full evaluation artifacts.

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
