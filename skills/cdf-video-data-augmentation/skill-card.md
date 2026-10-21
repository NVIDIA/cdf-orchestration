## Description: <br>
Run the CDF Video Data Augmentation Airflow workflow on Kubernetes or OSMO to create Cosmos video-to-video variants and auto-label both source and generated videos. <br>

This skill is ready for commercial/non-commercial use. <br>

## Owner
NVIDIA <br>

### License/Terms of Use: <br>
CC-BY-4.0 AND Apache 2.0 <br>
## Use Case: <br>
Developers and engineers use this skill to author and validate Video Data Augmentation payloads, preflight an existing Airflow controller, trigger and monitor runs on Kubernetes or OSMO, and verify or retrieve the generated Cosmos video variants and auto-labeling outputs. <br>

### Deployment Geography for Use: <br>
Global <br>

## Requirements / Dependencies: <br>
**Requires API Key or External Credential:** [Yes] <br>
**Credential Type(s):** [Cloud Credentials] <br>

Do not include secrets in prompts/logs/output; use least-privilege credentials; rotate keys as appropriate. <br>

## Known Risks and Mitigations: <br>
Risk: Review before execution as proposals could introduce incorrect or misleading guidance into skills. <br>
Mitigation: Review and scan skill before deployment. <br>

## Reference(s): <br>
- [Video Data Augmentation Getting Started](../../docs/video-data-augmentation/getting-started.md) <br>
- [Video Data Augmentation Payload Guide](../../docs/video-data-augmentation/payload-guide.md) <br>
- [setup-and-preflight.md](references/setup-and-preflight.md) <br>
- [payload-contract.md](references/payload-contract.md) <br>
- [airflow-direct-api.md](references/airflow-direct-api.md) <br>
- [outputs.md](references/outputs.md) <br>
- [troubleshooting.md](references/troubleshooting.md) <br>
- [cdf-augmentation skill (pinned commit)](https://github.com/NVIDIA/cdf-augmentation/tree/73c861c43a4952b6060c87265bf39f4abc48199a/skills/cdf-augmentation) <br>
- [cdf-auto-labeling skill (pinned commit)](https://github.com/NVIDIA/cdf-auto-labeling/tree/6c75c76c52a6c76c3866a38d30885e11123b0981/skills/cdf-auto-labeling) <br>


## Skill Output: <br>
**Output Type(s):** [Shell commands, API Calls, Files, Analysis] <br>
**Output Format:** [Markdown with inline bash code blocks and JSON payloads] <br>
**Output Parameters:** [1D] <br>
**Other Properties Related to Output:** [Payloads are shown to the user without secrets and require confirmation before a single run is submitted.] <br>

## Evaluation Agents Used: <br>
- Claude Code (`aws/anthropic/bedrock-claude-opus-4-8`) <br>
- Codex (`openai/openai/gpt-5.5`) <br>



## Evaluation Tasks: <br>
21 evaluation tasks (16 positive, 5 negative), one attempt per task, each run in an isolated sandbox pod in a `k8s-sandbox` environment. <br>

## Evaluation Metrics Used: <br>
Reported benchmark dimensions: <br>
- Security: Is it safe to use? <br>
- Correctness: Is the answer correct? <br>
- Discoverability: Was the right skill loaded when needed? <br>
- Effectiveness: Did the skill help complete the task? <br>
- Efficiency: Did it avoid wasted tool calls and token usage? <br>

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
| Overall | 93.0% — baseline ran, but no comparable score was available; uplift unavailable | 89.7% — baseline ran, but no comparable score was available; uplift unavailable |
| Security | 92.9% → 100.0% (+7.1 points) | 85.7% → 97.6% (+11.9 points) |
| Correctness | 37.1% → 93.3% (+56.2 points) | 45.7% → 85.7% (+40.0 points) |
| Discoverability | 93.3% — baseline ran, but no comparable score was available; uplift unavailable | 89.1% — baseline ran, but no comparable score was available; uplift unavailable |
| Effectiveness | 36.0% → 88.3% (+52.3 points) | 37.4% → 81.7% (+44.3 points) |
| Efficiency | 90.0% — baseline ran, but no comparable score was available; uplift unavailable | 94.6% — baseline ran, but no comparable score was available; uplift unavailable |

## Skill Version(s): <br>
1.0.0 (source: frontmatter) <br>

## Ethical Considerations: <br>
NVIDIA believes Trustworthy AI is a shared responsibility and we have established policies and practices to enable development for a wide array of AI applications. When downloaded or used in accordance with our terms of service, developers should work with their internal team to ensure this skill meets requirements for the relevant industry and use case and addresses unforeseen product misuse. <br>

(For Release on NVIDIA Platforms Only) <br>
Please report quality, risk, security vulnerabilities or NVIDIA AI Concerns [here](https://app.intigriti.com/programs/nvidia/nvidiavdp/detail). <br>
