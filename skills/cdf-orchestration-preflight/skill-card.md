## Description: <br>
Set up missing Linux prerequisites and audit whether a Cosmos Data Factory (CDF) Orchestration checkout and its Kubernetes or OSMO environment are ready to install the controller and run every packaged DAG. <br>

This skill is ready for commercial/non-commercial use. <br>

## Owner
NVIDIA <br>

### License/Terms of Use: <br>
CC-BY-4.0 AND Apache 2.0 <br>
## Use Case: <br>
Developers and platform engineers use this skill to set up Linux, Kubernetes, GPU, RWX/NFS storage, and credential prerequisites, and to audit install and per-DAG run readiness before deploying the CDF Orchestration controller or triggering workflows. <br>

### Deployment Geography for Use: <br>
Global <br>

## Requirements / Dependencies: <br>
**Requires API Key or External Credential:** [Yes] <br>
**Credential Type(s):** [API key, Cloud Credentials, Other [Kubernetes kubeconfig file]] <br>

Do not include secrets in prompts/logs/output; use least-privilege credentials; rotate keys as appropriate. <br>

## Known Risks and Mitigations: <br>
Risk: Review before execution as proposals could introduce incorrect or misleading guidance into skills. <br>
Mitigation: Review and scan skill before deployment. <br>

## Reference(s): <br>
- [Set up the Linux environment](references/bootstrap.md) <br>
- [NFS and RWX remediation](references/nfs-remediation.md) <br>
- [Readiness model](references/readiness-model.md) <br>
- [Getting Started Guide](../../docs/getting-started.md) <br>
- [Autonomous Vehicle Dataset Generation Prerequisites](../../docs/autonomous-vehicle-dataset-generation/prerequisites.md) <br>
- [Docker Engine installation](https://docs.docker.com/engine/install/) <br>
- [Kubernetes container runtimes](https://kubernetes.io/docs/setup/production-environment/container-runtimes/) <br>


## Skill Output: <br>
**Output Type(s):** [Analysis, Shell commands, Configuration instructions] <br>
**Output Format:** [Markdown with inline bash code blocks; optional JSON readiness report] <br>
**Output Parameters:** [1D] <br>
**Other Properties Related to Output:** [Reports install_readiness and run_readiness as independent gates (READY, PARTIAL, BLOCKED, UNVERIFIED); never prints credentials or secret contents.] <br>

## Evaluation Agents Used: <br>
- Claude Code (`aws/anthropic/bedrock-claude-opus-4-8`) <br>
- Codex (`openai/openai/gpt-5.5`) <br>



## Evaluation Tasks: <br>
14 evaluation tasks (10 positive, 4 negative), one attempt per task, each run in an isolated `k8s-sandbox` pod. <br>

## Evaluation Metrics Used: <br>
Reported benchmark dimensions: <br>
- Security: Is it safe to use? Scored from the `security` signal. <br>
- Correctness: Is the answer correct? Scored from the `accuracy` signal. <br>
- Discoverability: Was the right skill loaded when needed? Scored from the `skill_execution` signal. <br>
- Effectiveness: Did the skill help complete the task? Equal-weight mean of `goal_accuracy` and `behavior_check`. <br>
- Efficiency: Did it avoid wasted tool calls and token usage? Equal-weight mean of `skill_efficiency` and `token_efficiency`. <br>

Underlying evaluation signals used in this run: <br>
- `security`: Unsafe operations, secret leakage, and unauthorized access. <br>
- `skill_execution`: Whether the expected skill was selected, decoys were avoided, and the workflow executed. <br>
- `skill_efficiency`: Tool-call productivity. <br>
- `accuracy`: Final-answer correctness against the reference answer. <br>
- `goal_accuracy`: Whether the user's goal was achieved. <br>
- `behavior_check`: Whether the expected workflow behavior was followed. <br>
- `token_efficiency`: Actual uncached prompt plus completion usage. <br>



## Evaluation Results: <br>
| Measure | Claude Code (Baseline → Skill Uplift) | Codex (Baseline → Skill Uplift) |
|---|---:|---:|
| Overall | 85.8% (uplift unavailable) | 81.7% (uplift unavailable) |
| Security | 85.7% → 100.0% (+14.3 points) | 82.1% → 78.6% (-3.5 points) |
| Correctness | 12.9% → 75.7% (+62.8 points) | 32.9% → 82.9% (+50.0 points) |
| Discoverability | 96.5% (uplift unavailable) | 89.0% (uplift unavailable) |
| Effectiveness | 39.5% → 66.4% (+26.9 points) | 39.4% → 70.9% (+31.5 points) |
| Efficiency | 90.5% (uplift unavailable) | 87.4% (uplift unavailable) |

## Skill Version(s): <br>
1.1.2 (source: frontmatter) <br>

## Ethical Considerations: <br>
NVIDIA believes Trustworthy AI is a shared responsibility and we have established policies and practices to enable development for a wide array of AI applications. When downloaded or used in accordance with our terms of service, developers should work with their internal team to ensure this skill meets requirements for the relevant industry and use case and addresses unforeseen product misuse. <br>

(For Release on NVIDIA Platforms Only) <br>
Please report quality, risk, security vulnerabilities or NVIDIA AI Concerns [here](https://app.intigriti.com/programs/nvidia/nvidiavdp/detail). <br>
