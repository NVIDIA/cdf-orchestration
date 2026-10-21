# Skill Benchmark: cdf-image-attribute-augmentation

> ✅ **Overall verdict: PASS — Recommended for publication**

## Publication Recommendation

Recommended for publication based on the completed evaluation evidence in this report.

## Evaluation Metadata

- Skill: `cdf-image-attribute-augmentation`
- Evaluation date: 2026-10-20
- Evaluator version: `1.5.6`
- Agents: Claude Code (`aws/anthropic/bedrock-claude-opus-4-8`), Codex (`openai/openai/gpt-5.5`)
- Tasks: 13 evaluation tasks (11 positive, 2 negative)
- Dataset digest: `sha256:ae89be5ed6fc1b93dcd783e4dce32f759aa4fd98aaa966690f25cd8c8046a431` (skill-evaluator-dataset-snapshot/1)
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
| Overall | 85.7% — baseline ran, but no comparable score was available; uplift unavailable | 80.2% — baseline ran, but no comparable score was available; uplift unavailable |
| Security | 76.9% → 100.0% (+23.1 points) | 61.5% → 100.0% (+38.5 points) |
| Correctness | 26.2% → 89.2% (+63.0 points) | 24.6% → 76.9% (+52.3 points) |
| Discoverability | 82.7% — baseline ran, but no comparable score was available; uplift unavailable | 74.6% — baseline ran, but no comparable score was available; uplift unavailable |
| Effectiveness | 39.3% → 71.3% (+32.0 points) | 34.7% → 60.7% (+26.0 points) |
| Efficiency | 85.0% — baseline ran, but no comparable score was available; uplift unavailable | 89.0% — baseline ran, but no comparable score was available; uplift unavailable |

**How to read this table:** baseline is the same task attempted without the target skill. Scores are rounded to one decimal; threshold-adjacent values use additional precision so their displayed band matches the verdict. Uplift is derived from those displayed scores and shown in percentage points.

Example: `47.0% → 92.0% (+45.0 points)` means the skill-assisted run scored 92.0%, 45.0 percentage points above its 47.0% no-skill baseline.

A partial dimension was calculated from only the available configured signals; review the detailed report before relying on it.

## Token Usage

Actual Tier 3 execution usage is reported for every observed agent/case pair and both conditions.

| Agent | Dataset case | With skill | Without skill | Delta | Change | Coverage |
|---|---|---:|---:|---:|---:|---|
| claude-code | All cases | 2,184,264 | 4,679,704 | N/A | N/A | skill 13/13; base 12/13 |
| claude-code | iaa-augmentation-only-unsupported | 68,810 | 120,815 | -52,005 | -43.05% | skill 1/1; base 1/1 |
| claude-code | iaa-caption-only-unsupported | 69,062 | 277,206 | -208,144 | -75.09% | skill 1/1; base 1/1 |
| claude-code | iaa-defect-routing-negative | 213,101 | N/A | N/A | N/A | skill 1/1; base 0/1 |
| claude-code | iaa-e2e-external-positive | 69,825 | 1,899,724 | -1,829,899 | -96.32% | skill 1/1; base 1/1 |
| claude-code | iaa-e2e-internal-positive | 150,472 | 984,349 | -833,877 | -84.71% | skill 1/1; base 1/1 |
| claude-code | iaa-local-upload-positive | 199,493 | 185,249 | +14,244 | +7.69% | skill 1/1; base 1/1 |
| claude-code | iaa-no-dataset-negative | 109,754 | 319,778 | -210,024 | -65.68% | skill 1/1; base 1/1 |
| claude-code | iaa-no-secret-leak-security | 29,927 | 29,458 | +469 | +1.59% | skill 1/1; base 1/1 |
| claude-code | iaa-partial-distribution-negative | 439,053 | 149,310 | +289,743 | +194.05% | skill 1/1; base 1/1 |
| claude-code | iaa-platform-selection-positive | 101,441 | 89,900 | +11,541 | +12.84% | skill 1/1; base 1/1 |
| claude-code | iaa-status-results-positive | 361,732 | 287,677 | +74,055 | +25.74% | skill 1/1; base 1/1 |
| claude-code | iaa-variable-conditional-positive | 206,347 | 183,977 | +22,370 | +12.16% | skill 1/1; base 1/1 |
| claude-code | iaa-vda-routing-negative | 165,247 | 152,261 | +12,986 | +8.53% | skill 1/1; base 1/1 |
| codex | All cases | 1,738,474 | 2,485,627 | -747,153 | -30.06% | skill 13/13; base 13/13 |
| codex | iaa-augmentation-only-unsupported | 49,272 | 46,566 | +2,706 | +5.81% | skill 1/1; base 1/1 |
| codex | iaa-caption-only-unsupported | 208,494 | 311,820 | -103,326 | -33.14% | skill 1/1; base 1/1 |
| codex | iaa-defect-routing-negative | 388,197 | 287,630 | +100,567 | +34.96% | skill 1/1; base 1/1 |
| codex | iaa-e2e-external-positive | 112,755 | 83,324 | +29,431 | +35.32% | skill 1/1; base 1/1 |
| codex | iaa-e2e-internal-positive | 112,140 | 274,974 | -162,834 | -59.22% | skill 1/1; base 1/1 |
| codex | iaa-local-upload-positive | 180,254 | 311,878 | -131,624 | -42.20% | skill 1/1; base 1/1 |
| codex | iaa-no-dataset-negative | 70,248 | 41,694 | +28,554 | +68.48% | skill 1/1; base 1/1 |
| codex | iaa-no-secret-leak-security | 13,571 | 13,307 | +264 | +1.98% | skill 1/1; base 1/1 |
| codex | iaa-partial-distribution-negative | 46,026 | 83,254 | -37,228 | -44.72% | skill 1/1; base 1/1 |
| codex | iaa-platform-selection-positive | 96,057 | 80,269 | +15,788 | +19.67% | skill 1/1; base 1/1 |
| codex | iaa-status-results-positive | 321,280 | 895,900 | -574,620 | -64.14% | skill 1/1; base 1/1 |
| codex | iaa-variable-conditional-positive | 84,501 | 13,648 | +70,853 | +519.15% | skill 1/1; base 1/1 |
| codex | iaa-vda-routing-negative | 55,679 | 41,363 | +14,316 | +34.61% | skill 1/1; base 1/1 |
| ALL AGENTS | Dataset aggregate | 3,922,738 | 7,165,331 | N/A | N/A | skill 26/26; base 25/26 |

Prompt tokens include cached reads, so total tokens are `prompt + completion` (cached is not added twice). The Efficiency score uses `(prompt - cached) + completion`. N/A means the relevant trajectory counters were not available; coverage is never estimated.

## Tier Status

| Tier | Purpose | Status | Evidence |
|---|---|---|---|
| Tier 1 | Static validation | **PASSED WITH OBSERVATIONS** | 11 validator(s); 43 finding(s) |
| Tier 2 | Semantic deduplication | **PASSED** | 2 validator(s); 0 finding(s) |
| Tier 3 | Live agent evaluation | **PASS** | 2 agent(s); 13 task(s) |

## Findings and Observations

<details>
<summary>Show detailed findings and successful checks</summary>

- **MEDIUM** QUALITY/quality_correctness: No documented scripts in table format (`skills/cdf-image-attribute-augmentation/SKILL.md`)
- **MEDIUM** QUALITY/quality_correctness: Instructions don't mention 'run_script' (`skills/cdf-image-attribute-augmentation/SKILL.md`)
- **MEDIUM** QUALITY/quality_efficiency: Large skill (6401 tokens, recommended max <5000). Per agentskills.io, SKILL.md should be concise (~500 lines) — large skill bodies increase token cost after invocation; long or unfocused top-level descriptions can degrade agent routing accuracy (`skills/cdf-image-attribute-augmentation/SKILL.md`)
- **MEDIUM** QUALITY/quality_efficiency: Deeply nested references in airflow-direct-api.md (`skills/cdf-image-attribute-augmentation/SKILL.md`)
- **MEDIUM** SCHEMA/frontmatter_field_placement: Root field 'version' is ignored; use 'metadata.version' (`skills/cdf-image-attribute-augmentation/SKILL.md`)
- 38 additional finding(s) are available in the full evaluation artifacts.

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
