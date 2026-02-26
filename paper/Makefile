SHELL := /bin/bash

.PHONY: all validation simulate report clean

all: validation simulate report

validation:
	python tools/validation/run_validation.py --config configs/runs.yaml --artifacts-dir artifacts

simulate:
	@if [[ -f artifacts/validation/microbench_overlay.yaml ]]; then \
		python run.py --config configs/runs.yaml --artifacts-dir artifacts --validation-overlay artifacts/validation/microbench_overlay.yaml; \
	else \
		python run.py --config configs/runs.yaml --artifacts-dir artifacts; \
	fi

report:
	python report.py --config configs/runs.yaml --artifacts-dir artifacts

clean:
	rm -rf artifacts/validation
