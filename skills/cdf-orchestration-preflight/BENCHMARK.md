# Skill Benchmark: cdf-orchestration-preflight

> ✅ **Overall verdict: PASS — Recommended for publication**

## Publication Recommendation

Recommended for publication based on the completed evaluation evidence in this report.

## Evaluation Metadata

- Skill: `cdf-orchestration-preflight`
- Evaluation date: 2026-10-20
- Evaluator version: `1.5.6`
- Agents: Claude Code (`aws/anthropic/bedrock-claude-opus-4-8`), Codex (`openai/openai/gpt-5.5`)
- Tasks: 14 evaluation tasks (10 positive, 4 negative)
- Dataset digest: `sha256:da87380d621af7409004f0f0ec9b73435205584669d91aaed1c756c648cd2978` (skill-evaluator-dataset-snapshot/1)
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
| Overall | 85.8% — baseline ran, but no comparable score was available; uplift unavailable | 81.7% — baseline ran, but no comparable score was available; uplift unavailable |
| Security | 85.7% → 100.0% (+14.3 points) | 82.1% → 78.6% (-3.5 points) |
| Correctness | 12.9% → 75.7% (+62.8 points) | 32.9% → 82.9% (+50.0 points) |
| Discoverability | 96.5% — baseline ran, but no comparable score was available; uplift unavailable | 89.0% — baseline ran, but no comparable score was available; uplift unavailable |
| Effectiveness | 39.5% → 66.4% (+26.9 points) | 39.4% → 70.9% (+31.5 points) |
| Efficiency | 90.5% — baseline ran, but no comparable score was available; uplift unavailable | 87.4% — baseline ran, but no comparable score was available; uplift unavailable |

**How to read this table:** baseline is the same task attempted without the target skill. Scores are rounded to one decimal; threshold-adjacent values use additional precision so their displayed band matches the verdict. Uplift is derived from those displayed scores and shown in percentage points.

Example: `47.0% → 92.0% (+45.0 points)` means the skill-assisted run scored 92.0%, 45.0 percentage points above its 47.0% no-skill baseline.

A partial dimension was calculated from only the available configured signals; review the detailed report before relying on it.

## Token Usage

Actual Tier 3 execution usage is reported for every observed agent/case pair and both conditions.

| Agent | Dataset case | With skill | Without skill | Delta | Change | Coverage |
|---|---|---:|---:|---:|---:|---|
| claude-code | All cases | 5,181,784 | 3,050,160 | +2,131,624 | +69.89% | skill 14/14; base 14/14 |
| claude-code | audit-before-remediation | 386,094 | 299,247 | +86,847 | +29.02% | skill 1/1; base 1/1 |
| claude-code | av-ffmpeg-sidecar-missing | 337,718 | 221,866 | +115,852 | +52.22% | skill 1/1; base 1/1 |
| claude-code | live-unprofiled-dag | 296,440 | 152,740 | +143,700 | +94.08% | skill 1/1; base 1/1 |
| claude-code | managed-cluster-no-nfs | 242,960 | 258,670 | -15,710 | -6.07% | skill 1/1; base 1/1 |
| claude-code | missing-kubeconfig | 238,891 | 121,615 | +117,276 | +96.43% | skill 1/1; base 1/1 |
| claude-code | negative-author-custom-dag | 627,686 | 288,205 | +339,481 | +117.79% | skill 1/1; base 1/1 |
| claude-code | negative-controller-deploy | 421,633 | 284,327 | +137,306 | +48.29% | skill 1/1; base 1/1 |
| claude-code | negative-trigger-defect-run | 269,013 | 121,308 | +147,705 | +121.76% | skill 1/1; base 1/1 |
| claude-code | negative-workflow-run | 222,244 | 258,422 | -36,178 | -14.00% | skill 1/1; base 1/1 |
| claude-code | osmo-not-proven | 435,974 | 150,810 | +285,164 | +189.09% | skill 1/1; base 1/1 |
| claude-code | partial-one-gpu | 179,583 | 182,008 | -2,425 | -1.33% | skill 1/1; base 1/1 |
| claude-code | prebuilt-controller-image | 612,035 | 218,381 | +393,654 | +180.26% | skill 1/1; base 1/1 |
| claude-code | quick-start-boundary | 429,000 | 243,370 | +185,630 | +76.27% | skill 1/1; base 1/1 |
| claude-code | self-managed-nfs | 482,513 | 249,191 | +233,322 | +93.63% | skill 1/1; base 1/1 |
| codex | All cases | 3,881,599 | 3,567,859 | +313,740 | +8.79% | skill 14/14; base 14/14 |
| codex | audit-before-remediation | 183,469 | 151,776 | +31,693 | +20.88% | skill 1/1; base 1/1 |
| codex | av-ffmpeg-sidecar-missing | 236,394 | 174,811 | +61,583 | +35.23% | skill 1/1; base 1/1 |
| codex | live-unprofiled-dag | 245,139 | 55,925 | +189,214 | +338.34% | skill 1/1; base 1/1 |
| codex | managed-cluster-no-nfs | 465,909 | 246,435 | +219,474 | +89.06% | skill 1/1; base 1/1 |
| codex | missing-kubeconfig | 104,242 | 80,764 | +23,478 | +29.07% | skill 1/1; base 1/1 |
| codex | negative-author-custom-dag | 452,147 | 198,193 | +253,954 | +128.13% | skill 1/1; base 1/1 |
| codex | negative-controller-deploy | 544,963 | 1,198,400 | -653,437 | -54.53% | skill 1/1; base 1/1 |
| codex | negative-trigger-defect-run | 693,388 | 437,361 | +256,027 | +58.54% | skill 1/1; base 1/1 |
| codex | negative-workflow-run | 232,087 | 213,296 | +18,791 | +8.81% | skill 1/1; base 1/1 |
| codex | osmo-not-proven | 130,296 | 163,729 | -33,433 | -20.42% | skill 1/1; base 1/1 |
| codex | partial-one-gpu | 162,100 | 135,583 | +26,517 | +19.56% | skill 1/1; base 1/1 |
| codex | prebuilt-controller-image | 156,896 | 310,663 | -153,767 | -49.50% | skill 1/1; base 1/1 |
| codex | quick-start-boundary | 148,541 | 99,607 | +48,934 | +49.13% | skill 1/1; base 1/1 |
| codex | self-managed-nfs | 126,028 | 101,316 | +24,712 | +24.39% | skill 1/1; base 1/1 |
| ALL AGENTS | Dataset aggregate | 9,063,383 | 6,618,019 | +2,445,364 | +36.95% | skill 28/28; base 28/28 |

Prompt tokens include cached reads, so total tokens are `prompt + completion` (cached is not added twice). The Efficiency score uses `(prompt - cached) + completion`. N/A means the relevant trajectory counters were not available; coverage is never estimated.

## Tier Status

| Tier | Purpose | Status | Evidence |
|---|---|---|---|
| Tier 1 | Static validation | **PASSED WITH OBSERVATIONS** | 11 validator(s); 81 finding(s) |
| Tier 2 | Semantic deduplication | **PASSED** | 2 validator(s); 0 finding(s) |
| Tier 3 | Live agent evaluation | **PASS** | 2 agent(s); 14 task(s) |

## Findings and Observations

<details>
<summary>Show detailed findings and successful checks</summary>

- **MEDIUM** QUALITY/quality_correctness: No documented scripts in table format (`skills/cdf-orchestration-preflight/SKILL.md`)
- **MEDIUM** QUALITY/quality_correctness: Instructions don't mention 'run_script' (`skills/cdf-orchestration-preflight/SKILL.md`)
- **MEDIUM** QUALITY/quality_correctness: SKILL_SPEC recommended field missing: 'metadata.author' (`skills/cdf-orchestration-preflight/SKILL.md`)
- **MEDIUM** QUALITY/quality_efficiency: Deeply nested references in bootstrap.md (`skills/cdf-orchestration-preflight/SKILL.md`)
- **MEDIUM** SCHEMA/body_recommended_section: Missing recommended section: '## Instructions' (`skills/cdf-orchestration-preflight/SKILL.md`)
- 76 additional finding(s) are available in the full evaluation artifacts.

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
