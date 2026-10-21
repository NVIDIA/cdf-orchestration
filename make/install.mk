.PHONY: build build-sdg-controller install install-sdg-controller nfs sdg-controller sdg-workflow

nfs sdg-controller sdg-workflow:
	@:

build:
ifeq ($(filter sdg-controller sdg-workflow,$(MAKECMDGOALS)),)
	@echo "Usage: make build sdg-controller"
	@echo ""
	@echo "  make build sdg-controller   - Build the Airflow image, push it, and update deploy/values.yaml"
	@exit 1
else
	@$(MAKE) build-sdg-controller
endif

install:
ifeq ($(filter nfs,$(MAKECMDGOALS)),nfs)
	@$(MAKE) install-nfs
else ifeq ($(filter sdg-controller,$(MAKECMDGOALS)),sdg-controller)
	@$(MAKE) install-sdg-controller
else
	@echo "Usage: make install nfs|sdg-controller"
	@echo ""
	@echo "  make install nfs            - Install host NFS and StorageClass"
	@echo "  make install sdg-controller - Deploy SDG controller using the Airflow image in deploy/values.yaml"
	@exit 1
endif

# Image build/publish target
build-sdg-controller: ## [build sdg-controller] Build the Airflow image, push it, and write repository/tag into deploy/values.yaml
	@echo "=========================================="
	@echo "Building and publishing the Airflow image..."
	@echo "=========================================="
	@set -e; \
	if [ ! -f "$(SECRETS_ENV)" ]; then \
		echo "❌ Error: $(SECRETS_ENV) not found. Run make setup first."; \
		exit 1; \
	fi; \
	set -a; \
	. "./$(SECRETS_ENV)"; \
	set +a; \
	case "$(AIRFLOW_IMAGE_REPOSITORY)" in \
		*'<'*|*'>'*) echo "❌ Error: AIRFLOW_IMAGE_REPOSITORY is still the placeholder ($(AIRFLOW_IMAGE_REPOSITORY)). Set it in make/common.mk or on the command line to an NGC org/team path that NGC_API_KEY can push."; exit 1;; \
	esac; \
	IMAGE="$(AIRFLOW_IMAGE_REPOSITORY):$(AIRFLOW_IMAGE_TAG)"; \
	REGISTRY="$${IMAGE%%/*}"; \
	case "$$REGISTRY" in \
		*.*|*:*) ;; \
		*) echo "❌ Error: AIRFLOW_IMAGE_REPOSITORY ($(AIRFLOW_IMAGE_REPOSITORY)) has no registry host to push to."; exit 1;; \
	esac; \
	if [ -z "$${NGC_API_KEY:-}" ]; then \
		echo "❌ Error: NGC_API_KEY is not set in $(SECRETS_ENV). It is required to push $$IMAGE."; \
		exit 1; \
	fi; \
	echo ""; \
	echo "Step 1/3: Building Airflow image with DAGs and runtime dependencies..."; \
	echo "=========================================="; \
	echo "📦 Building image: $$IMAGE"; \
	docker build -t "$$IMAGE" -f docker/Dockerfile.airflow .; \
	echo "✅ Airflow image built: $$IMAGE"; \
	echo ""; \
	echo "Step 2/3: Pushing image to $$REGISTRY..."; \
	echo "=========================================="; \
	printf '%s' "$$NGC_API_KEY" | docker login "$$REGISTRY" --username '$$oauthtoken' --password-stdin; \
	docker push "$$IMAGE"; \
	echo "✅ Pushed image: $$IMAGE"; \
	echo ""; \
	echo "Step 3/3: Updating $(VALUES_DEV) with the published repository and tag..."; \
	echo "=========================================="; \
	sed -i -E \
	  -e 's|^([[:space:]]*defaultAirflowRepository:).*|\1 $(AIRFLOW_IMAGE_REPOSITORY)|' \
	  -e 's|^([[:space:]]*defaultAirflowTag:).*|\1 $(AIRFLOW_IMAGE_TAG)|' \
	  "$(VALUES_DEV)"; \
	echo "Updated $(VALUES_DEV): defaultAirflowRepository=$(AIRFLOW_IMAGE_REPOSITORY) defaultAirflowTag=$(AIRFLOW_IMAGE_TAG)"; \
	echo ""; \
	echo "=========================================="; \
	echo "✅ Published image: $$IMAGE"; \
	echo "=========================================="; \
	echo ""; \
	echo "Next step:"; \
	echo "  make install sdg-controller"

# Installation/Upgrade target
install-sdg-controller: ## [install sdg-controller] Install/upgrade SDG controller using the Airflow image in deploy/values.yaml
	@echo "=========================================="
	@echo "Installing/Upgrading for development..."
	@echo "=========================================="
	@set -e; \
	if [ ! -f "$(SECRETS_ENV)" ]; then \
		echo "❌ Error: $(SECRETS_ENV) not found. Run make setup first."; \
		exit 1; \
	fi; \
	set -a; \
	. "./$(SECRETS_ENV)"; \
	set +a; \
	if grep -Eq '^[[:space:]]*defaultAirflowRepository:.*[<>]' "$(VALUES_DEV)"; then \
		echo "❌ Error: defaultAirflowRepository in $(VALUES_DEV) is still the placeholder. Run make build sdg-controller first."; \
		exit 1; \
	fi; \
	echo ""; \
	echo "Deploying the Airflow image from $(VALUES_DEV)."; \
	echo "Rebuild with 'make build sdg-controller' only when DAGs, plugins, or runtime deps changed."; \
	echo ""; \
	echo "Step 1/3: Updating Helm chart dependencies..."; \
	echo "=========================================="; \
	$(HELM_DEV) dependency update $(CHART_PATH); \
	echo ""; \
	echo "Step 2/3: Uninstalling existing deployment and cleaning up namespace..."; \
	echo "=========================================="; \
	$(MAKE) uninstall; \
	echo ""; \
	echo "Step 3/3: Deploying to Kubernetes..."; \
	echo "=========================================="; \
	$(HELM_DEV) upgrade --install $(RELEASE_NAME) $(CHART_PATH) \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--values $(VALUES_DEV) \
		--values $(SECRET_VALUES_DEV) \
		--wait \
		--timeout 10m; \
	echo ""; \
	echo "=========================================="; \
	echo "✅ Installation complete!"; \
	echo "=========================================="; \
	echo ""; \
	echo "Access the UI:"; \
	echo "  make port-forward"; \
	echo "  Then open http://localhost:8080 (default credentials: admin / admin)"
