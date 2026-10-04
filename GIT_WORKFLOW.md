# Git Repository for Submission

Assignments that say "maintain a Git repository" are usually also graded on the
commit history. One giant `initial commit` looks like the project appeared
overnight. Build it up in logical steps instead.

## First-time setup

```bash
cd kafka-avro-orders
git init
git branch -M main
git config user.name  "Your Name"
git config user.email "your.email@university.edu"
```

## A commit history that tells the story

Stage the files in stages rather than all at once:

```bash
git add .gitignore README.md requirements.txt
git commit -m "chore: project scaffolding, dependencies and README skeleton"

git add docker-compose.yml scripts/create_topics.sh Makefile
git commit -m "infra: Kafka in KRaft mode, Schema Registry and Kafka UI via Docker Compose"

git add schemas/order.avsc
git commit -m "feat: define Order Avro schema (orderId, product, price)"

git add src/__init__.py src/config.py src/models.py
git commit -m "feat: configuration and Order model with transient/permanent error taxonomy"

git add src/producer.py
git commit -m "feat: Avro producer with Schema Registry integration and idempotent delivery"

git add src/aggregator.py tests/test_logic.py
git commit -m "feat: incremental running average using Welford's method, with unit tests"

git add src/consumer.py
git commit -m "feat: consumer with real-time aggregation and manual offset commits"

git add schemas/failed_order.avsc src/dlq.py
git commit -m "feat: dead letter queue with Avro error envelope and Kafka headers"

git add src/dlq_consumer.py
git commit -m "feat: DLQ inspector with replay support"

git add docs/
git commit -m "docs: demonstration runbook and git workflow"

git add -A
git commit -m "docs: complete README with architecture and design rationale"
```

## Push it

```bash
git remote add origin https://github.com/<username>/kafka-avro-orders.git
git push -u origin main
```

## Tag your submission

```bash
git tag -a v1.0 -m "Assignment submission: Kafka + Avro order pipeline"
git push origin v1.0
```

## Housekeeping

- Never commit `.venv/`, `__pycache__/` or `.aggregator_state.json` — `.gitignore`
  already excludes them. Check with `git status` before every commit.
- If you accidentally commit the virtualenv:
  ```bash
  git rm -r --cached .venv && git commit -m "chore: remove venv from tracking"
  ```
- Write commit messages in the imperative mood ("add retry logic", not "added
  retry logic") — it is the Git convention and it reads better in `git log`.
- If you work on this over several days, commit at the end of each session. A
  history spread across realistic dates is far more convincing than eleven
  commits with identical timestamps.
