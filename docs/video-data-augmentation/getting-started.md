# Video Data Augmentation Getting Started

Complete the shared [Getting Started Guide](../getting-started.md) through
opening the Airflow UI, then return here to run Video Data Augmentation.

The workflow processes each source video through 2 branches. The source
branch is auto-labeled directly. In parallel, video generation creates the
requested variants, enabled Cosmos Evaluator gates filter them, and the passing
variants enter a second auto-labeling branch. Each branch produces its own
training export; the workflow does not merge them into one dataset manifest.

> **OSMO: EXTERNAL ENDPOINTS ONLY:** Never use
> `external_services: false` with `video_data_augmentation_dag_osmo`. Supply
> every VLM, LLM, and video generation URL used by `cosmos`, `captioning`,
> `visual_qa`, and `reasoning`. Supply an external Cosmos Evaluator URL too
> whenever an evaluator check is enabled. The OSMO manifest cannot deploy any
> of these services.

## Choose an Execution Target

| Target | DAG ID | Service Mode |
| --- | --- | --- |
| Kubernetes | `video_data_augmentation_dag_k8s` | Internal or external inference services |
| OSMO | `video_data_augmentation_dag_osmo` | External inference services; external evaluator when checks are enabled |

## Hardware Requirements

Recommended GPUs on your workflow cluster:

| Component | **Kubernetes with external model endpoints** (`external_services: true`)<br>All checkers disabled | **Kubernetes with external model endpoints** (`external_services: true`)<br>3 checkers enabled | **Kubernetes with internal model services** (`external_services: false`)<br>All checkers disabled | **Kubernetes with internal model services** (`external_services: false`)<br>3 checkers enabled |
| --- | :---: | :---: | :---: | :---: |
| Processing steps (2, so the original-video and generated-video branches run at the same time) | 2 | 2 | 2 | 2 |
| Language model | | | 1 | 1 |
| Video generation | | | 2 | 2 |
| Hallucination checker | | 1 | | 1 |
| Attribute verification checker | | 1 | | 1 |
| Physical-plausibility checker | | 1 | | 1 |
| **Recommended total GPUs** | **2** | **5** | **5** | **8** |

Each checker you disable removes 1 GPU. With fewer than 2 GPUs for processing steps, the branches run one at a time and pods can wait long enough to time out.

**OSMO:** each task uses 1 GPU, 11 CPUs, 100 GiB RAM, and 100 GiB storage. Plan for 2 tasks at once.

Before a run, check free GPUs, current consumers, model/cache storage, pod placement, and available Airflow pool slots.

## Run the Workflow

1. **Upload Input Videos**:

   Upload 1 source video or a directory of source videos to S3. Directory
   discovery supports `.mp4`, `.avi`, `.mov`, `.mkv`, and `.webm` files. For
   example:

   ```text
   video-augmentation-input/
      intersection-001.mp4
      intersection-002.mp4
   ```

   A direct file path is also accepted. Start with 1 short video while
   validating a new deployment because VDA runs 2 full labeling branches and
   Cosmos video generation can take substantial GPU time.

2. **Create a Starter Payload**:

   For Kubernetes internal services, copy the checked-in structural example
   [`single_video_w_internal_endpooint.json`](../../payloads/dev/video_data_augmentation_dag/single_video_w_internal_endpooint.json).
   For external services, start from the complete external example in the
   [payload guide](payload-guide.md#external-services).
   Then set:

   - `input_path` to your video or video-directory S3 path.
   - `output_directory` to a writable output location, for example
     `s3://<your-output-bucket>/<your-workflow>/output`.
   - `max_videos` to a small positive number for the first run. Use `null`, `0`,
     or a negative integer when you intend to process every video.
   - `external_services` to `false` to start the services from the Kubernetes
     manifest, or `true` to use existing endpoints.

   **`external_services: false` is supported by
   `video_data_augmentation_dag_k8s` only.**

   Checked-in payload files contain development bucket paths and, in some
   cases, deployment-specific endpoints. Replace them; do not treat them as
   runnable defaults.

   With external services, supply the VLM, LLM, and video generation URLs in
   `cosmos`, plus the endpoint fields in `captioning`, `visual_qa`, and
   `reasoning`. Placeholder defaults can pass validation but cannot serve a
   run.

   `video_data_augmentation_dag_osmo` supports external endpoints only. OSMO
   cannot deploy the VLM, LLM, video generation, or Cosmos Evaluator
   services. Supply all inference endpoints and, when any evaluator check is
   enabled, an external `cosmos.cosmos_evaluator_service_url`. Refer to
   [OSMO Payloads: External Endpoints Only](payload-guide.md#osmo-payloads-external-endpoints-only).

   The checked-in payload disables physical-plausibility evaluation while
   retaining hallucination and attribute-verification checks. Omit
   `cosmos.cosmos_evaluator_service_url` to start the evaluator, or set
   it to an existing Arbitrator root URL.

   VDA uses its bundled VQA question bank. The `visual_qa.question_bank_file`
   value in the checked-in payload is accepted but is replaced when the DAG
   prepares each VQA task.

   A candidate rejected by an enabled Cosmos Evaluator gate is omitted from the
   generated-video labeling branch. The run can continue with the passing
   candidates, but fails if every candidate is rejected.

   Refer to the [Video Data Augmentation Payload Guide](payload-guide.md) for every
   field, lifecycle rules, endpoint requirements, and output paths.

3. **Review the Payload and Installation**:

   Review the completed JSON against the [payload guide](payload-guide.md).
   Confirm the service mode, real endpoint URLs, evaluator policy, and
   video-specific variable distribution. Check field names carefully: the
   runtime model ignores unknown fields in several sections, and placeholder
   endpoint defaults can pass schema validation but cannot serve a run.

   In the existing Airflow installation, verify that the selected DAG is
   loaded and unpaused with no relevant import error. Kubernetes runs require
   open slots in `default_pool`, `k8s_rtx6000_1`, and for external video
   generation, `cosmos_transfer_3_service_pool`. OSMO uses `osmo_rtx6000_1`,
   `default_pool`, and `cosmos_transfer_3_service_pool`.

4. **Start the Workflow from the UI**:

   > Only run one GPU-heavy workflow at a time unless the Airflow pools and
   > cluster capacity have been sized for concurrent runs.

   1. From the Airflow home page, select **Dags** in the left sidebar.
   2. Select `video_data_augmentation_dag_k8s`, or
      `video_data_augmentation_dag_osmo` for an external-endpoint OSMO run.
   3. Click the trigger button in the upper-right corner.
   4. Under **Run Parameters**, replace `payload` with the JSON from the
      previous step.
   5. Select **Trigger** to start the run.

   To trigger, monitor, and stop the run without the UI, follow
   [Run Workflows with the Airflow REST API](../airflow-rest-api.md) with
   `DAG_ID=video_data_augmentation_dag_k8s` (or `video_data_augmentation_dag_osmo`).

5. **Monitor or Stop an Active Run**:

   Monitor the original-video branch under `auto_labeling_input`, Cosmos
   generation under `cosmos_augmentation`, and the generated-video branch
   under `auto_labeling_output`.

   To cancel a run, open the active DagRun in **Grid** or **Graph** view, find
   the running task, and mark it as failed. This lets the DAG run service
   shutdown and clean up its resources.

   **Do not delete the DagRun or DAG** from the UI. Deleting a run bypasses the
   shutdown path and can leave stale Deployments, Services, or GPU pods.

6. **Inspect the Workflow Output**:

   Use the Airflow run ID to inspect
   `<output_directory>/<run_id>/`. The main prefixes are:

   - `cosmos/`: Generated videos plus Cosmos captions and metadata.
   - `auto_labeling_input/`: Labels generated for the source videos.
   - `auto_labeling_output/`: Labels generated for passing Cosmos variants.
   - `training_export/auto_labeling_input/`: Source-video training export.
   - `training_export/auto_labeling_output/`: Generated-video training export.
   - `output/report.json`: Run report, when reporting completed.

   A successful run requires the expected Cosmos candidate directories, at
   least 1 complete candidate, and non-empty output from both labeling and
   training-export branches. Account separately for candidates rejected by an
   evaluator or left incomplete. The current workflow does not combine the 2
   branches into one final dataset manifest.

## Fine-Tuning and Calibration (Optional)

Video Data Augmentation uses Cosmos Evaluator to assess generated videos before
auto-labeling. Its physical plausibility checker uses a learned classifier to
score temporal plausibility. The example payload disables this check; when
enabled, candidates that fail its quality gate are excluded from the
generated-video labeling branch.

The [Physical Plausibility Checker notebook](https://github.com/NVIDIA/cosmos-evaluator/blob/main/docs/tutorials/train_physical_plausibility_checker.ipynb)
walks through preparing labeled videos, extracting frozen video embeddings,
training or fine-tuning a lightweight classifier, calibrating its operating
threshold, and exporting a checkpoint bundle. Use validation examples from your
application to choose the threshold, then assess the checker on held-out test
videos before deployment.

To use the exported checker, configure its checkpoint in Cosmos Evaluator as
described in [Model Setup](https://github.com/NVIDIA/cosmos-evaluator/blob/main/docs/checks/physical_plausibility.md#model-setup),
then point `cosmos.cosmos_evaluator_service_url` to that deployment's Arbitrator
root URL. See the [Cosmos Evaluator settings](payload-guide.md#cosmos-evaluator)
for enabling the check and configuring the endpoint in this workflow.
