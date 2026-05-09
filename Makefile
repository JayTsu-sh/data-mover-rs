.PHONY: help build test clippy clippy-strict fmt fmt-fix check ci doc clean examples \
        e2e-local e2e-cifs e2e-nfs e2e-s3 e2e-network e2e-all \
        coverage audit-large audit-dispatch verify

help:
	@echo "data-mover-rs Makefile (Claude harness 稳定命令面)"
	@echo ""
	@echo "构建/检查："
	@echo "  build          cargo build --all-targets"
	@echo "  check          cargo check --all-targets"
	@echo "  examples       cargo build --examples (9 个)"
	@echo "  doc            cargo doc --no-deps"
	@echo ""
	@echo "质量："
	@echo "  fmt            cargo fmt --all -- --check"
	@echo "  fmt-fix        cargo fmt --all"
	@echo "  clippy         cargo clippy --all-targets (baseline 化, 见 .claude/skills/quality-clippy/)"
	@echo "  test           cargo test --workspace --no-fail-fast"
	@echo "  coverage       cargo llvm-cov (需要 cargo-llvm-cov)"
	@echo ""
	@echo "Skill 矩阵："
	@echo "  e2e-local      本地 backend skill (无外部依赖, CI 跑这个)"
	@echo "  e2e-cifs       CIFS backend skill (需 .env)"
	@echo "  e2e-nfs        NFS backend skill (需 .env)"
	@echo "  e2e-s3         S3 backend skill (需 .env)"
	@echo "  e2e-network    跑全部网络 backend (cifs+nfs+s3)"
	@echo "  e2e-all        跑全部 e2e (含 local)"
	@echo "  audit-large    审大文件 (列 >800 行的 src/*.rs)"
	@echo "  audit-dispatch 审 StorageEnum 分派完整性"
	@echo ""
	@echo "组合："
	@echo "  ci             fmt + clippy + test + examples + e2e-local"
	@echo "  verify         ci + audit-large + audit-dispatch (commit 前自检)"

build:
	cargo build --all-targets

check:
	cargo check --all-targets

examples:
	cargo build --examples

doc:
	cargo doc --no-deps

fmt:
	cargo fmt --all -- --check

fmt-fix:
	cargo fmt --all

clippy:
	python3 .claude/skills/quality-clippy/scripts/run.py

clippy-strict:
	cargo clippy --all-targets -- -D warnings

test:
	cargo test --workspace --no-fail-fast

coverage:
	cargo llvm-cov --workspace --html

e2e-local:
	python3 .claude/skills/e2e-local/scripts/run.py

e2e-cifs:
	python3 .claude/skills/e2e-cifs/scripts/run.py

e2e-nfs:
	python3 .claude/skills/e2e-nfs/scripts/run.py

e2e-s3:
	python3 .claude/skills/e2e-s3/scripts/run.py

e2e-network: e2e-cifs e2e-nfs e2e-s3

e2e-all: e2e-local e2e-network

audit-large:
	python3 .claude/skills/quality-large-file-audit/scripts/run.py

audit-dispatch:
	python3 .claude/skills/quality-dispatch-coverage/scripts/run.py

ci: fmt clippy test examples e2e-local

verify: ci audit-large audit-dispatch

clean:
	cargo clean
