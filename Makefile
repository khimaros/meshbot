.PHONY: check clean help

help:
	@echo "available targets:"
	@echo "  check - run type checking with mypy"
	@echo "  clean - remove generated files"

check:
	python -m mypy meshbot.py

clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} +
	find . -name ".mypy_cache" -type d -exec rm -rf {} +
