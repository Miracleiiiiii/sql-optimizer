# Spark AI Optimizer

Spark AI Optimizer is an MVP service for diagnosing historical Spark on YARN applications by `applicationId`.

It collects data from Spark History Server and YARN ResourceManager, normalizes metrics, runs deterministic diagnosis rules, generates Spark parameter recommendations, and optionally calls an OpenAI-compatible Responses API to produce a structured report.

## Quick Start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy config\application.example.yaml config\application.yaml
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Submit an analysis:

```powershell
Invoke-RestMethod -Method Post "http://localhost:8000/api/v1/analysis" `
  -ContentType "application/json" `
  -Body '{"applicationId":"application_1778340258140_0024","forceRefresh":false}'
```

If `OPENAI_API_KEY` is not set, the service still returns a rule-based report.

## Configuration

Copy `config/application.example.yaml` to `config/application.yaml` and adjust cluster addresses.

