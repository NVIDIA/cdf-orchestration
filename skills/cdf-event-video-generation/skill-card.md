## Description: <br>
Run the CDF Orchestration Event Video Generation DAG on Kubernetes or OSMO (external endpoints only on OSMO) for Cosmos3 image-to-video anomaly generation, auto-labeling, and anomaly dataset generation from seed images. <br>

This skill is ready for commercial/non-commercial use. <br>

## Owner
NVIDIA <br>

### License/Terms of Use: <br>
CC-BY-4.0 AND Apache 2.0 <br>
## Use Case: <br>
Developers and engineers use this skill to collect inputs, run readiness checks, render and validate payloads, trigger and monitor the Event Video Generation DAG, and retrieve results, producing synthetic anomaly video datasets (for example person-falling, fighting, fire/smoke, or shoplifting clips) from seed images for safety, surveillance, and physical AI training. <br>

### Deployment Geography for Use: <br>
Global <br>

## Requirements / Dependencies: <br>
**Requires API Key or External Credential:** [Yes] <br>
**Credential Type(s):** [API key, Cloud Credentials, Other [Basic auth], Other [Kubernetes cluster credential file]] <br>

Do not include secrets in prompts/logs/output; use least-privilege credentials; rotate keys as appropriate. <br>

## Known Risks and Mitigations: <br>
Risk: Review before execution as proposals could introduce incorrect or misleading guidance into skills. <br>
Mitigation: Review and scan skill before deployment. <br>

## Reference(s): <br>
- [Setup and preflight](references/setup-and-preflight.md) <br>
- [Event Video Generation payload contract](references/payload-contract.md) <br>
- [Direct Airflow API](references/airflow-direct-api.md) <br>
- [Event Video Generation output layout](references/outputs.md) <br>
- [Troubleshooting](references/troubleshooting.md) <br>
- [cdf-augmentation component skill (pinned commit)](https://github.com/NVIDIA/cdf-augmentation/tree/73c861c43a4952b6060c87265bf39f4abc48199a/skills/cdf-augmentation) <br>
- [cdf-auto-labeling component skill (pinned commit)](https://github.com/NVIDIA/cdf-auto-labeling/tree/6c75c76c52a6c76c3866a38d30885e11123b0981/skills/cdf-auto-labeling) <br>


## Skill Output: <br>
**Output Type(s):** [Shell commands, API Calls, Files, Analysis] <br>
**Output Format:** [Markdown with inline bash code blocks and JSON payload files] <br>
**Output Parameters:** [1D] <br>
**Other Properties Related to Output:** [Runs are triggered only after the user confirms the rendered payload; results are downloaded only after explicit user confirmation] <br>

## Evaluation Agents Used: <br>
- Claude Code (`aws/anthropic/bedrock-claude-opus-4-8`) <br>
- Codex (`openai/openai/gpt-5.5`) <br>



## Evaluation Tasks: <br>
15 evaluation tasks (15 positive), one attempt per task, each run in an isolated sandbox pod in the `k8s-sandbox` environment and compared against a no-skill baseline (evaluator version 1.5.6, evaluated 2026-10-20). <br>

## Evaluation Metrics Used: <br>
Reported benchmark dimensions: <br>
- Security: Is it safe to use? Scored from the `security` signal. <br>
- Correctness: Is the answer correct? Scored from the `accuracy` signal. <br>
- Discoverability: Was the right skill loaded when needed? Scored from the `skill_execution` signal. <br>
- Effectiveness: Did the skill help complete the task? Equal-weight mean of `goal_accuracy` and `behavior_check`. <br>
- Efficiency: Did it avoid wasted tool calls and token usage? 50% `skill_efficiency` (tool-call productivity) and 50% `token_efficiency`. <br>

Underlying evaluation signals used in this run: <br>
- `security`: Unsafe operations, secret leakage, and unauthorized access. <br>
- `skill_execution`: Whether the expected skill was selected, decoys were avoided, and the workflow executed. <br>
- `skill_efficiency`: Tool-call productivity (legacy wire id; routing is scored under Discoverability). <br>
- `accuracy`: Final-answer correctness against the reference answer. <br>
- `goal_accuracy`: Whether the user's goal was achieved. <br>
- `behavior_check`: Whether the expected workflow behavior was followed. <br>
- `token_efficiency`: Actual uncached prompt plus completion usage (50% of Efficiency). <br>



## Evaluation Results: <br>
| Measure | Claude Code (Baseline → Skill Uplift) | Codex (Baseline → Skill Uplift) |
|---|---:|---:|
| Overall | 89.3% — baseline ran, but no comparable score was available; uplift unavailable | 87.6% — baseline ran, but no comparable score was available; uplift unavailable |
| Security | 66.7% → 100.0% (+33.3 points) | 50.0% → 93.3% (+43.3 points) |
| Correctness | 40.0% → 98.7% (+58.7 points) | 32.0% → 96.0% (+64.0 points) |
| Discoverability | 78.9% — baseline ran, but no comparable score was available; uplift unavailable | 78.0% — baseline ran, but no comparable score was available; uplift unavailable |
| Effectiveness | 45.9% → 81.1% (+35.2 points) | 36.8% → 80.2% (+43.4 points) |
| Efficiency | 87.9% — baseline ran, but no comparable score was available; uplift unavailable | 90.3% — baseline ran, but no comparable score was available; uplift unavailable |

## Skill Version(s): <br>
1.0.2 (source: frontmatter) <br>

## Ethical Considerations: <br>
NVIDIA believes Trustworthy AI is a shared responsibility and we have established policies and practices to enable development for a wide array of AI applications. When downloaded or used in accordance with our terms of service, developers should work with their internal team to ensure this skill meets requirements for the relevant industry and use case and addresses unforeseen product misuse. <br>

(For Release on NVIDIA Platforms Only) <br>
Please report quality, risk, security vulnerabilities or NVIDIA AI Concerns [here](https://app.intigriti.com/programs/nvidia/nvidiavdp/detail). <br>
