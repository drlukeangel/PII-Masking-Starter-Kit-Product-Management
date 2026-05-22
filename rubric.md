# PII Rubric

Every column in your dataset falls into one of four buckets. The
treatment for each bucket is fixed. The work isn't *how* to mask — it's
*deciding which bucket each column belongs in*.

Get the rubric right, run the Glue job. The rest is bookkeeping.

---

## The four buckets

### 1. Direct identifiers → **hash with rotating salt**

A direct identifier picks out an individual with no other information
needed. Email, phone, government ID, device serial, login.

**Treatment.** SHA-256 with a salt that rotates every 90 days. Salt
lives in Secrets Manager, never in the repo. The output is irreversible
and unjoinable across rotation windows — exactly the property you want.

**Example columns:** `operator_email`, `tool_serial`, `device_imei`,
`badge_number`.

### 2. Quasi-identifiers → **tokenize to stable random string**

A quasi-identifier alone doesn't pick out a person, but combined with
two or three others, it can. Name, employee ID, IP, MAC address.

**Treatment.** Stable per-value tokenization — `Luke Angel` always maps
to `op_3f9c8b1d`, but the mapping table lives in a separately-encrypted
bucket nobody but the privacy officer can read. Joins still work
*within the masked dataset*; identification doesn't work *across* the
masked dataset and any other.

**Example columns:** `operator_name`, `operator_id`, `mac_address`.

### 3. Sensitive attributes → **generalize**

Sensitive attributes are properties that carry risk even without
identifying anyone — location, biometrics, religion, health, sexual
orientation, salary. Hashing doesn't help here; the *value itself* is
the privacy issue.

**Treatment.** Generalize until the value loses re-identification
power. Specific tactics:

| Attribute kind | Generalization |
| --- | --- |
| GPS coordinates | Snap to 0.01° grid (≈ 1.1 km), or to job-site centroid |
| Age | Five-year bins |
| Salary | Quartile bucket |
| Timestamps | Round to nearest hour for behavioral analysis |
| Free-text | Run through Presidio NER, redact entities |

**Example columns:** `gps_lat`, `gps_lon`, `birth_date`, `notes_freetext`.

### 4. Behavioral / non-PII → **keep**

Sensor readings, click events, durations, counts, derived metrics that
have no identifier attached. These power the product. Don't touch them.

**Example columns:** `battery_pct`, `torque_nm`, `usage_minutes`,
`error_code`, `firmware_version`.

---

## The rubric, applied to tool telemetry

| Column                | Bucket                    | Treatment                       |
| --------------------- | ------------------------- | ------------------------------- |
| `event_id`            | Direct (event)            | Keep — synthetic event UUID    |
| `event_ts`            | Sensitive (timestamp)     | Round to hour                  |
| `tool_serial`         | Direct                    | SHA-256 + salt                  |
| `tool_model`          | Behavioral                | Keep                            |
| `firmware_version`    | Behavioral                | Keep                            |
| `operator_id`         | Quasi                     | Stable tokenize                 |
| `operator_email`      | Direct                    | SHA-256 + salt                  |
| `operator_name`       | Quasi                     | Stable tokenize                 |
| `job_site_id`         | Quasi                     | Stable tokenize                 |
| `job_site_address`    | Sensitive                 | Drop street; keep city + state  |
| `gps_lat`, `gps_lon`  | Sensitive (location)      | Snap to 0.01° grid              |
| `battery_pct`         | Behavioral                | Keep                            |
| `torque_nm`           | Behavioral                | Keep                            |
| `usage_minutes`       | Behavioral                | Keep                            |
| `error_code`          | Behavioral                | Keep                            |

---

## The three questions to ask before adding a column

When someone wants to add a new column to the pipeline, you don't argue
about whether it's "PII." You ask three questions in order:

1. **Could this value, by itself, identify a specific person?**
   Yes → direct identifier → hash.

2. **Could this value, combined with two or three other columns in this
   dataset, identify a person?** Yes → quasi-identifier → tokenize.

3. **Is the value itself the privacy concern — location, biometric,
   health, salary, etc. — even with no name attached?** Yes →
   sensitive → generalize.

If all three are no, it's behavioral. Keep it.

That's the whole rubric. Print it. Tape it next to your monitor.
