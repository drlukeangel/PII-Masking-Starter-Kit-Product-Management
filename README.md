# PII Masking Starter Kit for Product Managers

> A reference rubric and pipeline I developed while leading the data-engineering
> work for a connected-products team. Open-sourced as a starting template for PMs
> and engineering leaders running the same playbook.

The kit is **the smallest credible PII masking stack a data-engineering team
needs to ship a compliant pipeline.** A four-bucket rubric, a runnable PySpark
job, an AWS DataBrew recipe, and a verify script that fails CI when the rubric
ever drifts from the output.

- **Five files, one rubric.** No notebook — verification is a script.
- Stack: `python` · `aws glue` (PySpark) · `aws databrew` · `pandas`
- Pairs with: **AWS Glue Studio**, **AWS DataBrew**, **S3**, **Athena**

---

## How teams use this

The kit is organized by *audience*. Different roles read different files:

- **Engineering managers** — fork as a starting template for your team's
  data-pipeline repo. The rubric and verify script are the load-bearing
  artifacts; the rest is reference shape you'll adapt.
- **Product managers** — read `rubric.md` and stop there. The rubric is
  the conversation, not the code.
- **Data engineers** — `glue/pii_masking_job.py` is a runnable example.
  Lift the structure, swap in your own schema, keep the rubric.
- **Privacy / Legal partners** — read `rubric.md` and audit `verify.py`.
  The verify script is the contract: if it passes, the rubric is honored.

The repo's job is to **make those conversations cheaper.** Every team I've
led running this loop has saved a quarter's worth of "what counts as PII"
debates by handing the rubric across the table on day one.

---

## Why this exists

Most teams handle PII three ways: ignore it (illegal), hash everything
(useless), or argue about it for six weeks before a single byte moves
(expensive). None of those scale.

This kit is the minimal opinionated alternative: a **rubric** that says
exactly what gets hashed / tokenized / generalized / kept, a **Glue job**
that does it, a **DataBrew recipe** for the analyst-friendly version,
and a **verify script** that fails CI if the rubric ever drifts from the
output.

## What's in the box

| File                          | Job                                                 |
| ----------------------------- | --------------------------------------------------- |
| `rubric.md`                   | The PII rubric — categories × treatment, one page   |
| `data/generate_synthetic.py`  | Generate a fake tool-telemetry dataset with PII     |
| `data/sample_tool_telemetry.csv` | 200 rows of synthetic data, ready to run        |
| `glue/pii_masking_job.py`     | PySpark Glue job — production path                  |
| `databrew/recipe.json`        | DataBrew recipe — analyst-friendly path             |
| `verify.py`                   | Post-mask invariants check; fails the build on drift|

That's the whole kit. No frameworks, no platform, no $40K licensing fee.

---

## The rubric, in one paragraph

PII isn't one thing. It's four. **Direct identifiers** (operator email,
serial number) get **hashed with a rotating salt**. **Quasi-identifiers**
(name, employee ID) get **tokenized** to a stable random string so joins
still work. **Sensitive attributes** (location, biometric, health) get
**generalized** (location snapped to a 0.01° grid; ages bucketed in
five-year bins). **Behavioral data** with no identifier attached
(battery level, usage minutes) is **kept**.

Full table in [`rubric.md`](rubric.md).

---

## Quick start

```bash
git clone https://github.com/drlukeangel/PII-Masking-Starter-Kit-Product-Management.git
cd PII-Masking-Starter-Kit-Product-Management
python -m venv .venv && source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt

# 1. Generate a fresh synthetic dataset (or use the included sample)
python data/generate_synthetic.py --rows 1000 --out data/tool_telemetry.csv

# 2. Run the Glue job locally (PySpark must be installed)
spark-submit glue/pii_masking_job.py \
  --input data/tool_telemetry.csv \
  --output data/tool_telemetry_masked.csv \
  --salt "$(openssl rand -hex 32)"

# 3. Verify the masking matches the rubric
python verify.py --input data/tool_telemetry_masked.csv --rubric rubric.md
```

The verify script returns exit code 0 if the rubric is satisfied,
non-zero otherwise. Wire it into CI on the data-pipeline repo so the
build fails when someone adds a column without thinking through which
bucket it belongs in.

---

## Why tool telemetry?

The synthetic dataset isn't e-commerce customers — it's **industrial
tool telemetry**: connected drills and torque wrenches sending readings
to the cloud, tagged with the operator who used them and the job site
they were on. The PII surface looks like this:

- `tool_serial` — direct identifier of the device (and indirectly, who
  bought it)
- `operator_id`, `operator_email`, `operator_name` — direct PII
- `gps_lat`, `gps_lon` — sensitive (location)
- `job_site_address` — quasi-identifier
- `battery_pct`, `torque_nm`, `usage_minutes` — behavioral, no PII

That's a real PII surface anyone working on a fleet-of-connected-things
product hits in week two. The rubric handles each.

---

## Using the Glue job in production

The job is written as a runnable PySpark script — it works
locally with `spark-submit` *and* as an AWS Glue job. To deploy:

1. Upload `glue/pii_masking_job.py` to S3 (e.g. `s3://your-bucket/glue/`).
2. Create a Glue Job pointing at it, with two arguments:
   - `--input s3://your-bucket/raw/tool_telemetry/`
   - `--output s3://your-bucket/masked/tool_telemetry/`
   - `--salt $(aws secretsmanager get-secret-value --secret-id pii/salt --query SecretString --output text)`
3. Set the Glue job's IAM role to read `raw/` and write `masked/`.
4. Schedule it (EventBridge → Glue, or a Step Functions workflow).

The output partitioning matches the input — masked data lands in the
same shape, just with PII columns rewritten.

---

## Using the DataBrew recipe (analyst path)

If the analyst on your team prefers the visual builder:

1. Open AWS DataBrew.
2. Create a dataset pointing at your raw data in S3.
3. Create a new project, then **Import recipe** from `databrew/recipe.json`.
4. Apply, preview, run.

The recipe encodes the same rubric. It's not as fast as the Glue job,
but the visual breadcrumb trail makes it easier for non-engineers to
audit. Most teams end up running both — DataBrew for sample audits,
Glue for production scale.

---

## CI integration

```yaml
# .github/workflows/pii-rubric.yml
name: pii-rubric
on: [pull_request]
jobs:
  verify:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r requirements.txt
      - run: python verify.py --input data/sample_tool_telemetry.csv --rubric rubric.md
```

The verify script reads `rubric.md`, checks the masked output against
each rule, and returns exit code 1 if anything fails. Catch rubric
drift in PR, not in a regulator's letter.

---

## When you outgrow this

This kit covers the first 80% so you can decide which 20% you actually
need. Heavier options worth knowing:

- **[Privacera](https://privacera.com/)** — enterprise data-access
  governance, integrates with Glue and Lake Formation.
- **[Immuta](https://www.immuta.com/)** — policy-as-code data masking,
  Snowflake-heavy.
- **[Microsoft Presidio](https://github.com/microsoft/presidio)** —
  open-source PII detection + masking; useful for free-text fields the
  rubric here doesn't handle.
- **AWS Macie** — PII *discovery* in S3, not masking. Run it on your raw
  bucket to surface columns the rubric missed.

This kit is the smallest useful thing. Graduate when it stops fitting.

---

## License

MIT.

## Maintainer

Luke Angel · [lukeangel.co](https://lukeangel.co)
