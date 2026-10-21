# Skill Benchmark: cdf-video-data-augmentation

> ✅ **Overall verdict: PASS — Recommended for publication**

## Publication Recommendation

Recommended for publication based on the completed evaluation evidence in this report.

## Evaluation Metadata

- Skill: `cdf-video-data-augmentation`
- Evaluation date: 2026-10-20
- Evaluator version: `1.5.6`
- Agents: Claude Code (`aws/anthropic/bedrock-claude-opus-4-8`), Codex (`openai/openai/gpt-5.5`)
- Tasks: 21 evaluation tasks (16 positive, 5 negative)
- Dataset digest: `sha256:3e630c8191a34b59d215c6cad8c34972de2d6012dc3124eb268311b9e98d4d26` (skill-evaluator-dataset-snapshot/1)
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
| Overall | 93.0% — baseline ran, but no comparable score was available; uplift unavailable | 89.7% — baseline ran, but no comparable score was available; uplift unavailable |
| Security | 92.9% → 100.0% (+7.1 points) | 85.7% → 97.6% (+11.9 points) |
| Correctness | 37.1% → 93.3% (+56.2 points) | 45.7% → 85.7% (+40.0 points) |
| Discoverability | 93.3% — baseline ran, but no comparable score was available; uplift unavailable | 89.1% — baseline ran, but no comparable score was available; uplift unavailable |
| Effectiveness | 36.0% → 88.3% (+52.3 points) | 37.4% → 81.7% (+44.3 points) |
| Efficiency | 90.0% — baseline ran, but no comparable score was available; uplift unavailable | 94.6% — baseline ran, but no comparable score was available; uplift unavailable |

**How to read this table:** baseline is the same task attempted without the target skill. Scores are rounded to one decimal; threshold-adjacent values use additional precision so their displayed band matches the verdict. Uplift is derived from those displayed scores and shown in percentage points.

Example: `47.0% → 92.0% (+45.0 points)` means the skill-assisted run scored 92.0%, 45.0 percentage points above its 47.0% no-skill baseline.

A partial dimension was calculated from only the available configured signals; review the detailed report before relying on it.

## Token Usage

Actual Tier 3 execution usage is reported for every observed agent/case pair and both conditions.

| Agent | Dataset case | With skill | Without skill | Delta | Change | Coverage |
|---|---|---:|---:|---:|---:|---|
| claude-code | All cases | 3,561,219 | 6,776,406 | -3,215,187 | -47.45% | skill 21/21; base 21/21 |
| claude-code | vda-augmentation-component-routing | 217,062 | 152,088 | +64,974 | +42.72% | skill 1/1; base 1/1 |
| claude-code | vda-cleanup-does-not-mask-failure | 104,787 | 91,333 | +13,454 | +14.73% | skill 1/1; base 1/1 |
| claude-code | vda-component-reference-inaccessible | 68,408 | 122,879 | -54,471 | -44.33% | skill 1/1; base 1/1 |
| claude-code | vda-custom-dag-routing | 30,374 | 215,220 | -184,846 | -85.89% | skill 1/1; base 1/1 |
| claude-code | vda-event-generation-routing-negative | 255,940 | 123,617 | +132,323 | +107.04% | skill 1/1; base 1/1 |
| claude-code | vda-full-workflow-component-routing | 69,995 | 216,201 | -146,206 | -67.63% | skill 1/1; base 1/1 |
| claude-code | vda-generation-only-unsupported | 467,296 | 246,013 | +221,283 | +89.95% | skill 1/1; base 1/1 |
| claude-code | vda-ignored-fields-negative | 147,213 | 186,394 | -39,181 | -21.02% | skill 1/1; base 1/1 |
| claude-code | vda-labeling-component-routing | 29,956 | 29,712 | +244 | +0.82% | skill 1/1; base 1/1 |
| claude-code | vda-minimum-compute-all-checkers-positive | 66,342 | 246,112 | -179,770 | -73.04% | skill 1/1; base 1/1 |
| claude-code | vda-minimum-compute-no-plausibility-positive | 65,725 | 60,182 | +5,543 | +9.21% | skill 1/1; base 1/1 |
| claude-code | vda-missing-dag-no-deploy | 392,583 | 251,642 | +140,941 | +56.01% | skill 1/1; base 1/1 |
| claude-code | vda-no-input-negative | 102,169 | 254,384 | -152,215 | -59.84% | skill 1/1; base 1/1 |
| claude-code | vda-no-secret-leak-security | 29,642 | 29,431 | +211 | +0.72% | skill 1/1; base 1/1 |
| claude-code | vda-orchestration-setup-routing | 170,417 | 217,938 | -47,521 | -21.80% | skill 1/1; base 1/1 |
| claude-code | vda-osmo-internal-services-negative | 151,845 | 309,324 | -157,479 | -50.91% | skill 1/1; base 1/1 |
| claude-code | vda-payload-layer-routing | 98,257 | 29,997 | +68,260 | +227.56% | skill 1/1; base 1/1 |
| claude-code | vda-render-external-k8s-positive | 388,791 | 461,235 | -72,444 | -15.71% | skill 1/1; base 1/1 |
| claude-code | vda-render-internal-k8s-positive | 295,097 | 600,423 | -305,326 | -50.85% | skill 1/1; base 1/1 |
| claude-code | vda-render-osmo-positive | 303,378 | 544,310 | -240,932 | -44.26% | skill 1/1; base 1/1 |
| claude-code | vda-retrieval-routing-negative | 105,942 | 2,387,971 | -2,282,029 | -95.56% | skill 1/1; base 1/1 |
| codex | All cases | 2,256,324 | 5,641,776 | -3,385,452 | -60.01% | skill 21/21; base 21/21 |
| codex | vda-augmentation-component-routing | 13,983 | 24,556 | -10,573 | -43.06% | skill 1/1; base 1/1 |
| codex | vda-cleanup-does-not-mask-failure | 46,703 | 84,516 | -37,813 | -44.74% | skill 1/1; base 1/1 |
| codex | vda-component-reference-inaccessible | 49,646 | 57,033 | -7,387 | -12.95% | skill 1/1; base 1/1 |
| codex | vda-custom-dag-routing | 29,916 | 13,554 | +16,362 | +120.72% | skill 1/1; base 1/1 |
| codex | vda-event-generation-routing-negative | 64,621 | 13,722 | +50,899 | +370.93% | skill 1/1; base 1/1 |
| codex | vda-full-workflow-component-routing | 218,944 | 14,624 | +204,320 | +1397.16% | skill 1/1; base 1/1 |
| codex | vda-generation-only-unsupported | 240,379 | 112,962 | +127,417 | +112.80% | skill 1/1; base 1/1 |
| codex | vda-ignored-fields-negative | 48,380 | 69,045 | -20,665 | -29.93% | skill 1/1; base 1/1 |
| codex | vda-labeling-component-routing | 13,589 | 13,544 | +45 | +0.33% | skill 1/1; base 1/1 |
| codex | vda-minimum-compute-all-checkers-positive | 70,079 | 629,588 | -559,509 | -88.87% | skill 1/1; base 1/1 |
| codex | vda-minimum-compute-no-plausibility-positive | 47,392 | 46,050 | +1,342 | +2.91% | skill 1/1; base 1/1 |
| codex | vda-missing-dag-no-deploy | 640,536 | 2,372,694 | -1,732,158 | -73.00% | skill 1/1; base 1/1 |
| codex | vda-no-input-negative | 47,794 | 68,969 | -21,175 | -30.70% | skill 1/1; base 1/1 |
| codex | vda-no-secret-leak-security | 29,942 | 13,432 | +16,510 | +122.92% | skill 1/1; base 1/1 |
| codex | vda-orchestration-setup-routing | 46,957 | 61,317 | -14,360 | -23.42% | skill 1/1; base 1/1 |
| codex | vda-osmo-internal-services-negative | 204,614 | 1,180,654 | -976,040 | -82.67% | skill 1/1; base 1/1 |
| codex | vda-payload-layer-routing | 29,783 | 84,489 | -54,706 | -64.75% | skill 1/1; base 1/1 |
| codex | vda-render-external-k8s-positive | 87,104 | 169,145 | -82,041 | -48.50% | skill 1/1; base 1/1 |
| codex | vda-render-internal-k8s-positive | 112,048 | 117,360 | -5,312 | -4.53% | skill 1/1; base 1/1 |
| codex | vda-render-osmo-positive | 86,885 | 158,006 | -71,121 | -45.01% | skill 1/1; base 1/1 |
| codex | vda-retrieval-routing-negative | 127,029 | 336,516 | -209,487 | -62.25% | skill 1/1; base 1/1 |
| ALL AGENTS | Dataset aggregate | 5,817,543 | 12,418,182 | -6,600,639 | -53.15% | skill 42/42; base 42/42 |

Prompt tokens include cached reads, so total tokens are `prompt + completion` (cached is not added twice). The Efficiency score uses `(prompt - cached) + completion`. N/A means the relevant trajectory counters were not available; coverage is never estimated.

## Tier Status

| Tier | Purpose | Status | Evidence |
|---|---|---|---|
| Tier 1 | Static validation | **PASSED WITH OBSERVATIONS** | 11 validator(s); 27 finding(s) |
| Tier 2 | Semantic deduplication | **PASSED** | 2 validator(s); 0 finding(s) |
| Tier 3 | Live agent evaluation | **PASS** | 2 agent(s); 21 task(s) |

## Findings and Observations

<details>
<summary>Show detailed findings and successful checks</summary>

- **MEDIUM** QUALITY/quality_correctness: No documented scripts in table format (`skills/cdf-video-data-augmentation/SKILL.md`)
- **MEDIUM** QUALITY/quality_correctness: Instructions don't mention 'run_script' (`skills/cdf-video-data-augmentation/SKILL.md`)
- **MEDIUM** QUALITY/quality_correctness: SKILL_SPEC recommended field missing: 'metadata.author' (`skills/cdf-video-data-augmentation/SKILL.md`)
- **MEDIUM** QUALITY/quality_correctness: SKILL_SPEC recommended field missing: 'metadata.tags' (`skills/cdf-video-data-augmentation/SKILL.md`)
- **MEDIUM** QUALITY/quality_efficiency: Deeply nested references in airflow-direct-api.md (`skills/cdf-video-data-augmentation/SKILL.md`)
- 22 additional finding(s) are available in the full evaluation artifacts.

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
