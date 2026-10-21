## Description: <br>
Run the CDF Orchestration Image Attribute Augmentation DAG on Kubernetes for person-crop clothing augmentation, attribute search, and augmented dataset generation. <br>

This skill is ready for commercial/non-commercial use. <br>

## Owner
NVIDIA <br>

### License/Terms of Use: <br>
CC-BY-4.0 AND Apache-2.0 <br>
## Use Case: <br>
Developers and engineers use this skill to prepare person-crop inputs, build and validate payloads, run readiness checks, trigger and monitor the Image Attribute Augmentation DAG, and retrieve and summarize results. The DAG generates controlled clothing and attribute variations of person-crop datasets for physical AI and person re-identification training. <br>

### Deployment Geography for Use: <br>
Global <br>

## Requirements / Dependencies: <br>
**Requires API Key or External Credential:** [Yes] <br>
**Credential Type(s):** [API key, Cloud Credentials, Other [Kubernetes cluster credential file], Other [Basic auth]] <br>

Do not include secrets in prompts/logs/output; use least-privilege credentials; rotate keys as appropriate. <br>

## Known Risks and Mitigations: <br>
Risk: Review before execution as proposals could introduce incorrect or misleading guidance into skills. <br>
Mitigation: Review and scan skill before deployment. <br>

## Reference(s): <br>
- [setup-and-preflight.md](references/setup-and-preflight.md) <br>
- [payload-contract.md](references/payload-contract.md) <br>
- [outputs.md](references/outputs.md) <br>
- [troubleshooting.md](references/troubleshooting.md) <br>
- [airflow-direct-api.md](references/airflow-direct-api.md) <br>
- [cdf-augmentation skill (pinned component documentation)](https://github.com/NVIDIA/cdf-augmentation/tree/73c861c43a4952b6060c87265bf39f4abc48199a/skills/cdf-augmentation) <br>
- [cdf-auto-labeling skill (pinned component documentation)](https://github.com/NVIDIA/cdf-auto-labeling/tree/6c75c76c52a6c76c3866a38d30885e11123b0981/skills/cdf-auto-labeling) <br>


## Skill Output: <br>
**Output Type(s):** [Shell commands, API Calls, Configuration instructions, Analysis, Files] <br>
**Output Format:** [Markdown with inline bash code blocks and JSON payloads] <br>
**Output Parameters:** [1D] <br>
**Other Properties Related to Output:** [None] <br>

## Evaluation Agents Used: <br>
- Claude Code (`aws/anthropic/bedrock-claude-opus-4-8`) <br>
- Codex (`openai/openai/gpt-5.5`) <br>



## Evaluation Tasks: <br>
13 evaluation tasks (11 positive, 2 negative), one attempt per task, each run in an isolated sandbox pod. <br>

## Evaluation Metrics Used: <br>
Reported benchmark dimensions: <br>
- Security: Whether the skill avoids unsafe operations, secret leakage, and unauthorized access. <br>
- Correctness: Final-answer correctness against the reference answer. <br>
- Discoverability: Whether the right skill was loaded when needed. <br>
- Effectiveness: Whether the skill helped complete the task: goal completion and expected workflow adherence. <br>
- Efficiency: Whether the agent avoided wasted tool calls and token usage. <br>

Underlying evaluation signals used in this run: <br>
- `security`: Unsafe operations, secret leakage, and unauthorized access. <br>
- `skill_execution`: Whether the expected skill was selected, decoys were avoided, and the workflow executed. <br>
- `skill_efficiency`: Tool-call productivity. <br>
- `accuracy`: Final-answer correctness against the reference answer. <br>
- `goal_accuracy`: Whether the user's goal was achieved. <br>
- `behavior_check`: Whether the expected workflow behavior was followed. <br>
- `token_efficiency`: Actual uncached prompt plus completion token usage. <br>



## Evaluation Results: <br>
| Measure | Claude Code (Baseline → Skill Uplift) | Codex (Baseline → Skill Uplift) |
|---|---:|---:|
| Overall | 85.7% | 80.2% |
| Security | 76.9% → 100.0% (+23.1 points) | 61.5% → 100.0% (+38.5 points) |
| Correctness | 26.2% → 89.2% (+63.0 points) | 24.6% → 76.9% (+52.3 points) |
| Discoverability | 82.7% | 74.6% |
| Effectiveness | 39.3% → 71.3% (+32.0 points) | 34.7% → 60.7% (+26.0 points) |
| Efficiency | 85.0% | 89.0% |

## Skill Version(s): <br>
1.0.1 (source: frontmatter) <br>

## Ethical Considerations: <br>
NVIDIA believes Trustworthy AI is a shared responsibility and we have established policies and practices to enable development for a wide array of AI applications. When downloaded or used in accordance with our terms of service, developers should work with their internal team to ensure this skill meets requirements for the relevant industry and use case and addresses unforeseen product misuse. <br>

(For Release on NVIDIA Platforms Only) <br>
Please report quality, risk, security vulnerabilities or NVIDIA AI Concerns [here](https://app.intigriti.com/programs/nvidia/nvidiavdp/detail). <br>
