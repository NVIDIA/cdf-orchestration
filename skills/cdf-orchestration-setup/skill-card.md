## Description: <br>
Deploy, install, reinstall, redeploy, upgrade, and connect to the CDF Orchestration Airflow controller after the cdf-orchestration-preflight audit reports installation readiness, covering make setup, controller image builds, Helm installation, controller connection, port-forwarding, and post-install verification. <br>

This skill is ready for commercial/non-commercial use. <br>

## Owner
NVIDIA <br>

### License/Terms of Use: <br>
CC-BY-4.0 AND Apache-2.0 <br>
## Use Case: <br>
Developers and infrastructure engineers who need to deploy, redeploy, connect to, and verify the CDF Orchestration Airflow controller on a Kubernetes cluster after preflight checks pass, so they can run synthetic data generation workflows. <br>

### Deployment Geography for Use: <br>
Global <br>

## Requirements / Dependencies: <br>
**Requires API Key or External Credential:** [Yes] <br>
**Credential Type(s):** [API key, Cloud Credentials, Other [Basic auth], Other [kubeconfig file]] <br>

Do not include secrets in prompts/logs/output; use least-privilege credentials; rotate keys as appropriate. <br>

## Known Risks and Mitigations: <br>
Risk: Review before execution as proposals could introduce incorrect or misleading guidance into skills. <br>
Mitigation: Review and scan skill before deployment. <br>

## Reference(s): <br>
- [SDG Controller Connection](references/controller-connection.md) <br>
- [Deploy the Controller](references/deploy-controller.md) <br>
- [Internal Model Startup Timeouts](references/model-startup-timeouts.md) <br>
- [Controller and Model-Service Topologies](references/topologies.md) <br>


## Skill Output: <br>
**Output Type(s):** [Shell commands, Configuration instructions, Analysis] <br>
**Output Format:** [Markdown with inline bash code blocks] <br>
**Output Parameters:** [1D] <br>
**Other Properties Related to Output:** [Ends with a deployment report: preflight report used, installation readiness, topology, commands executed, controller image tag, Airflow URL, controller checks, and unresolved deployment warnings] <br>

## Evaluation Agents Used: <br>
- Claude Code (`aws/anthropic/bedrock-claude-opus-4-8`) <br>
- Codex (`openai/openai/gpt-5.5`) <br>



## Evaluation Tasks: <br>
11 evaluation tasks (9 positive, 2 negative), one attempt per task, each run in an isolated sandbox pod (environment: k8s-sandbox). <br>

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
- `skill_efficiency`: Tool-call productivity (routing is scored under Discoverability). <br>
- `accuracy`: Final-answer correctness against the reference answer. <br>
- `goal_accuracy`: Whether the user's goal was achieved. <br>
- `behavior_check`: Whether the expected workflow behavior was followed. <br>
- `token_efficiency`: Actual uncached prompt plus completion usage. <br>



## Evaluation Results: <br>
| Measure | Claude Code (Baseline → Skill Uplift) | Codex (Baseline → Skill Uplift) |
|---|---:|---:|
| Overall | 85.8% — baseline ran, but no comparable score was available; uplift unavailable | 74.9% — baseline ran, but no comparable score was available; uplift unavailable |
| Security | 90.9% → 100.0% (+9.1 points) | 68.2% → 100.0% (+31.8 points) |
| Correctness | 20.0% → 83.6% (+63.6 points) | 21.8% → 70.9% (+49.1 points) |
| Discoverability | 96.4% — baseline ran, but no comparable score was available; uplift unavailable | 58.3% — baseline ran, but no comparable score was available; uplift unavailable |
| Effectiveness | 22.4% → 58.6% (+36.2 points) | 16.7% → 52.3% (+35.6 points) |
| Efficiency | 90.3% — baseline ran, but no comparable score was available; uplift unavailable | 93.0% — baseline ran, but no comparable score was available; uplift unavailable |

## Skill Version(s): <br>
1.1.3 (source: frontmatter) <br>

## Ethical Considerations: <br>
NVIDIA believes Trustworthy AI is a shared responsibility and we have established policies and practices to enable development for a wide array of AI applications. When downloaded or used in accordance with our terms of service, developers should work with their internal team to ensure this skill meets requirements for the relevant industry and use case and addresses unforeseen product misuse. <br>

(For Release on NVIDIA Platforms Only) <br>
Please report quality, risk, security vulnerabilities or NVIDIA AI Concerns [here](https://app.intigriti.com/programs/nvidia/nvidiavdp/detail). <br>
