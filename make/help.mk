.PHONY: help

# A target is listed when its rule line carries "## description". Prefix the
# description with "[usage]" to show the documented command form instead of
# the target name, e.g. "install-nfs: ## [install nfs] Install ...".
help:
	@echo "SDG Workflow Controller - Available Commands"
	@echo ""
	@awk 'BEGIN { FS = ":.*## " } /^[A-Za-z0-9_.-]+:.*## / { name = $$1; desc = $$2; if (match(desc, /^\[[^]]+\] */)) { name = substr(desc, 2, index(desc, "]") - 2); desc = substr(desc, RLENGTH + 1) } printf "  make %-24s - %s\n", name, desc }' $(MAKEFILE_LIST)
