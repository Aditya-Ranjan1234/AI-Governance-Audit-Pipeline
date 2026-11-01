Estimating recommended-tier costs and planning the data requirements table.

## Governance Audit Pipeline: 5 producers × 5 frameworks (7 slides)

### Slide 1: Introduction

* Problem: Evidence for AI governance and compliance sits in separate systems (identity, cluster, data pipelines, model registry, LLM gateway). Audits are periodic and manual, so violations such as an unapproved model promotion or PII in prompts are found late.  
* Idea: Stream governance events from 5 producers into Kafka. Evaluate 16 rules in real time, map each hit to 5 frameworks, score the risk, store results in PostgreSQL and show them in Tableau. Everything runs on AWS.  
* Producers: Keycloak (identity), Kubernetes audit (infrastructure), Airflow \+ OpenLineage (pipeline, lineage, drift/bias jobs), model registry (model lifecycle), LLM gateway (inference and prompts).  
* Frameworks: NIST AI RMF 1.0, ISO/IEC 42001:2023, EU AI Act, NIST SP 800-53 Rev. 5, GDPR.  
* Contribution: A reproducible event-to-framework compliance pipeline that covers AI-specific controls, with measured latency and detection quality.

### Slide 2: Dataset description and EDA results

The data is generated, so describe it as a generated corpus. The volumes below are design targets, not results, so replace them with your actual counts.

Dataset description

| Source | Content | Target volume (7-day run) |
| ----- | ----- | ----- |
| Keycloak | Login, logout, errors, admin and role events | \~50k |
| Kubernetes audit | API requests, RBAC changes, secret reads | \~300k |
| Airflow \+ OpenLineage | Task states, run events, dataset lineage, DRIFT\_METRIC and BIAS\_METRIC | \~100k |
| Model registry | Register, approve, promote events | \~5k |
| LLM gateway | Prompt and response metadata, model ID, user ID | \~200k |
| Injected violations | 16 rules × \~50 labeled instances, each tagged with scenario\_id | \~800 (≈0.1–2% of events) |

* Common schema: source, event\_type, actor, resource, timestamp, payload JSONB.  
* Supporting public data: Loghub (log anomaly baseline), UCI Adult (bias metrics), Faker-generated PII prompts.

EDA results to fill in

| EDA item | Chart | Result |
| ----- | ----- | ----- |
| Events per producer | Bar | \_\_\_ |
| Events per hour | Time series | \_\_\_ |
| Event-type distribution | Bar | \_\_\_ |
| Violations per rule | Bar | \_\_\_ |
| Violations per framework | Bar | \_\_\_ |
| Severity split | Pie or bar | \_\_\_ |
| Risk-score distribution | Histogram | \_\_\_ |
| Drift (PSI) over time | Line | \_\_\_ |
| Bias gap per model | Bar | \_\_\_ |
| PII entity types in prompts | Bar | \_\_\_ |

### Slide 3: Literature review (27 papers \+ 5 standards)

I wrote these from memory. Open every link and confirm the title, year and venue before you submit.

| \# | Paper | Year | Link | Theme |
| ----- | ----- | ----- | ----- | ----- |
| 1 | Kreps et al., *Kafka: a Distributed Messaging System for Log Processing* | 2011 | [pdf](https://notes.stephenholiday.com/Kafka.pdf) | Streaming |
| 2 | Wang et al., *Building a Replicated Logging System with Apache Kafka* | 2015 | [doi](https://doi.org/10.14778/2824032.2824063) | Streaming |
| 3 | Wang et al., *Consistency and Completeness: Rethinking Distributed Stream Processing in Apache Kafka* | 2021 | [doi](https://doi.org/10.1145/3448016.3457556) | Streaming |
| 4 | Sax et al., *Streams and Tables: Two Sides of the Same Coin* | 2018 | [link](https://www.confluent.io/blog/streams-tables-two-sides-same-coin/) | Streaming |
| 5 | Carbone et al., *Apache Flink: Stream and Batch Processing in a Single Engine* | 2015 | [pdf](http://sites.computer.org/debull/A15dec/p28.pdf) | Streaming |
| 6 | Karimov et al., *Benchmarking Distributed Stream Data Processing Systems* | 2018 | [arXiv](https://arxiv.org/abs/1802.08496) | Evaluation |
| 7 | Chandola et al., *Anomaly Detection: A Survey* | 2009 | [doi](https://doi.org/10.1145/1541880.1541882) | Anomaly detection |
| 8 | Pang et al., *Deep Learning for Anomaly Detection: A Review* | 2021 | [arXiv](https://arxiv.org/abs/2007.02500) | Anomaly detection |
| 9 | Liu et al., *Isolation Forest* | 2008 | [doi](https://doi.org/10.1109/ICDM.2008.17) | Risk scoring |
| 10 | Du et al., *DeepLog* | 2017 | [doi](https://doi.org/10.1145/3133956.3134015) | Log anomaly |
| 11 | He et al., *Drain: An Online Log Parsing Approach* | 2017 | [doi](https://doi.org/10.1109/ICWS.2017.13) | Log parsing |
| 12 | Zhu et al., *Tools and Benchmarks for Automated Log Parsing* | 2019 | [arXiv](https://arxiv.org/abs/1811.03509) | Log parsing |
| 13 | Guo et al., *LogBERT* | 2021 | [arXiv](https://arxiv.org/abs/2103.04475) | Log anomaly |
| 14 | Le & Zhang, *Log-based Anomaly Detection with Deep Learning: How Far Are We?* | 2022 | [arXiv](https://arxiv.org/abs/2202.04301) | Log anomaly |
| 15 | Zhu et al., *Loghub: A Large Collection of System Log Datasets* | 2020 | [arXiv](https://arxiv.org/abs/2008.06448) | Dataset |
| 16 | Han et al., *UNICORN: Runtime Provenance-Based Detector* | 2020 | [arXiv](https://arxiv.org/abs/2001.01525) | Provenance |
| 17 | Cheney et al., *Provenance in Databases: Why, How, and Where* | 2009 | [doi](https://doi.org/10.1561/1900000006) | Lineage |
| 18 | Herschel et al., *A Survey on Provenance: What for? What form? What from?* | 2017 | [doi](https://doi.org/10.1007/s00778-017-0486-1) | Lineage |
| 19 | Shastri et al., *Analyzing the Impact of GDPR on Storage Systems* | 2019 | [arXiv](https://arxiv.org/abs/1910.00728) | GDPR |
| 20 | Shamim et al., *XI Commandments of Kubernetes Security* | 2020 | [arXiv](https://arxiv.org/abs/2006.15275) | Kubernetes |
| 21 | NIST SP 800-207, *Zero Trust Architecture* | 2020 | [doi](https://doi.org/10.6028/NIST.SP.800-207) | Access control |
| 22 | Mitchell et al., *Model Cards for Model Reporting* | 2019 | [arXiv](https://arxiv.org/abs/1810.03993) | AI governance |
| 23 | Gebru et al., *Datasheets for Datasets* | 2021 | [arXiv](https://arxiv.org/abs/1803.09010) | AI governance |
| 24 | Raji et al., *Closing the AI Accountability Gap* | 2020 | [arXiv](https://arxiv.org/abs/2001.00973) | AI audit |
| 25 | Rabanser et al., *Failing Loudly: Dataset Shift Detection* | 2019 | [arXiv](https://arxiv.org/abs/1810.11953) | Drift |
| 26 | Gama et al., *A Survey on Concept Drift Adaptation* | 2014 | [doi](https://doi.org/10.1145/2523813) | Drift |
| 27 | Sculley et al., *Hidden Technical Debt in ML Systems* | 2015 | [NeurIPS](https://proceedings.neurips.cc/paper/2015/hash/86df7dcfd896fcaf2674f757a2463eba-Abstract.html) | ML-ops |

Standards and regulations (the 5 frameworks)

| Document | Link |
| ----- | ----- |
| NIST AI RMF 1.0 | [doi](https://doi.org/10.6028/NIST.AI.100-1) |
| ISO/IEC 42001:2023 | [ISO](https://www.iso.org/standard/81230.html) |
| Regulation (EU) 2024/1689 (AI Act) | [EUR-Lex](https://eur-lex.europa.eu/eli/reg/2024/1689/oj) |
| NIST SP 800-53 Rev. 5 | [doi](https://doi.org/10.6028/NIST.SP.800-53r5) |
| Regulation (EU) 2016/679 (GDPR) | [EUR-Lex](https://eur-lex.europa.eu/eli/reg/2016/679/oj) |

### Slide 4: Summary of literature

| Theme | Papers | What they give you | Gap |
| ----- | ----- | ----- | ----- |
| Streaming platforms | 1–6 | Durable logs, exactly-once, windowing, benchmarks | No mapping from streams to compliance controls |
| Anomaly and log detection | 7–15 | Isolation Forest, DeepLog, LogBERT, log datasets | Built for system faults, not control violations |
| Provenance and lineage | 16–18 | Audit trails, lineage models | Not linked to live rule evaluation |
| Regulation-specific security | 19–21 | GDPR storage impact, Kubernetes security, zero trust | One framework or one component each |
| AI documentation and audit | 22–24 | Model cards, datasheets, accountability practice | Process-oriented, with no real-time evidence stream |
| Drift and ML-ops | 25–27 | Drift detection methods, ML technical debt | Metrics are not tied to governance rules |
| Frameworks | 5 standards | Control catalogues | Documents, not executable systems |

Gap statement: To my knowledge, no single work combines multi-source event streaming, executable rules mapped across AI and security/privacy frameworks, risk scoring and a dashboard, evaluated for latency and detection quality. Search IEEE Xplore and Google Scholar for "continuous compliance monitoring", "AI audit streaming" and "policy-as-code" before you claim novelty.

### Slide 5: Objectives

1. Define a unified governance event schema across 5 producers.  
2. Stream events into Amazon MSK with one topic per producer and consumer-group scaling.  
3. Build a rule engine with 16 core rules mapped to 5 frameworks, using windowed detection and stateful joins.  
4. Add risk scoring: a severity × frequency baseline, then an Isolation Forest, with the two compared.  
5. Persist results to RDS PostgreSQL and build Tableau dashboards (framework status, risk trend, incident history).  
6. Evaluate precision and recall, p50/p95/p99 latency, throughput and resilience to pod and broker failure.

### Slide 6: Proposed methodology

Pipeline: 5 producers → 5 MSK topics → Kafka Streams on EKS (rules \+ risk) → compliance-events, risk-scores, audit-summary → sink service or MSK Connect → RDS PostgreSQL → Tableau.

| Phase | Work |
| ----- | ----- |
| 1\. Schema and rules | Common event schema, R1–R16 rule table, framework mapping table |
| 2\. Infrastructure | Terraform: VPC, IAM (IRSA), MSK (TLS \+ SASL/IAM), RDS, EKS with audit logging |
| 3\. Producers | Keycloak SPI, K8s audit via Fluent Bit, Airflow listener and OpenLineage transport, SageMaker EventBridge → Lambda, Bedrock invocation logs → Lambda |
| 4\. Processing | Kafka Streams: merge 5 streams, windowed counts, KTable joins (dataset owners, model approvals), rule evaluation, risk score |
| 5\. Persistence and dashboard | JDBC sink, then Tableau on RDS |
| 6\. Evaluation | Inject labeled violations, compute precision/recall per rule, measure detected\_at − ingested\_at, kill a pod or broker and compare offsets |

| Consumer (receiver) | Reads | Writes | Runs on |
| ----- | ----- | ----- | ----- |
| Stream processor | 5 raw topics | 3 derived topics | EKS, 3 replicas, one consumer group |
| Sink | derived topics | RDS tables | EKS service (or MSK Connect) |
| Tableau | RDS | none | Desktop or EC2 |

### Slide 7: Percentage of work completed

These are suggested values, so edit them to your real status.

| Work package | % done |
| ----- | ----- |
| Schema, rule table, framework mapping | 90 |
| Literature review | 80 |
| AWS foundation (VPC, IAM, MSK, RDS, EKS) | 60 |
| Producers integrated (n of 5\) | n/5 × 100 |
| Stream processor and rules (n of 16\) | n/16 × 100 |
| Risk scoring (ML) | 20 |
| Sink and Tableau | 30 |
| Evaluation | 10 |
| Report and paper | 30 |

---

## Estimated budget

These are rough monthly on-demand figures in USD. Prices vary by region (Mumbai usually runs higher), so confirm with the AWS Pricing Calculator before you commit.

| Item | Lean prototype | Full evaluation setup |
| ----- | ----- | ----- |
| MSK | 3× kafka.t3.small ≈ \$100–120 | 3× kafka.m5.large ≈ \$330–460 |
| EKS control plane | \$73 | \$73 |
| EKS nodes | 2× t3.medium (spot) ≈ \$20–30 | 3× m5.xlarge ≈ \$400+ |
| RDS PostgreSQL | One db.t3.medium, single-AZ, 2 databases ≈ \$60–75 | db.r6g.large Multi-AZ ≈ \$250–300 |
| NAT gateway | 1 ≈ \$35–45 | 2 ≈ \$70–90 |
| Airflow | Self-hosted on EKS ≈ \$0 extra | MWAA small ≈ \$350 |
| Sink | Own consumer on EKS ≈ \$0 | MSK Connect 1 MCU ≈ \$80 |
| Lambda, EventBridge, CloudWatch | ≈ \$10–20 | ≈ \$20–40 |
| Bedrock (small model, test prompts) | ≈ \$5–20 | ≈ \$20–50 |
| S3, ECR | ≈ \$5 | ≈ \$10 |
| Tableau | \$0 (student licence) | \$0 |
| Total | ≈ \$320–400 / month | ≈ \$1,500–2,000 / month |

How to keep it under about \$200 in total

* Develop locally with Docker Compose (Kafka, Keycloak, Airflow, Postgres, kind). It costs \$0.  
* Deploy to AWS only for the last 1–2 weeks (integration, evaluation, demo screenshots).  
* Run a single node group and a single NAT gateway, and tear everything down with terraform destroy after each session.  
* Skip MWAA (about \$350 a month) and MSK Connect. Self-host Airflow on EKS and write a small sink consumer.  
* Check for AWS credits for new accounts or student programs. Programs change, so verify current terms.  
* Set a Budgets alarm at \$50 and \$100 on day one.

---

## Data needed

| Data | Purpose | Source | Cost |
| ----- | ----- | ----- | ----- |
| Keycloak events | Rules R1–R3 | Generated by a script that logs in, fails and changes roles in a test realm | Free |
| K8s audit logs | Rules R4–R6 | Real EKS audit logs, plus scripted kubectl violations | Free |
| Pipeline and lineage events | R7, R8 | Real Airflow runs with the OpenLineage provider, plus a seeded dataset-owner table | Free |
| Drift and bias metrics | R9, R10 | A DAG computing PSI/KS and demographic-parity gap on a model's inputs and predictions; shift injected on purpose | Free |
| Training and test data for that model | Drift and bias source | UCI Adult (bias); a standard drift benchmark or your own injected covariate shift | Free |
| Model registry events | R11–R13 | A small model (e.g. Adult classifier) registered, approved and promoted through SageMaker, with some promotions deliberately unapproved | Free to low |
| Prompt and response logs | R14–R16 | Test prompts through Bedrock; PII injected with Faker (names, emails, phone numbers); some calls to a non-approved model ID | Low |
| Log benchmark | Anomaly-scoring comparison | Loghub (HDFS, BGL) | Free |
| Ground-truth labels | Precision and recall | scenario\_id and expected rule\_id written by your injection scripts | Free |
| Rule mapping table | Rule → framework | Hand-built from the 5 framework documents above | Free |

Minimum viable data: 500k–1M events over a week, at least 30–50 injected instances per rule, and 3–5 days of clean baseline traffic before injection so the Isolation Forest has normal behaviour to learn from.

I can turn all of this into a .pptx deck if you want.  
Searched the web, read a page

### 1\. Rules to check

Thresholds are starting defaults to tune on your baseline data. "Window" means event-time windows with a grace period.

| ID | Producer | Check (exact condition) | Window / state | Severity |
| ----- | ----- | ----- | ----- | ----- |
| R1 | Keycloak | Count of LOGIN\_ERROR per userId ≥ 5 | 5-min tumbling, 1-min grace | Medium |
| R2 | Keycloak | Admin event grants a role in {model-approver, registry-admin, gateway-admin} outside the change window | Lookup table of change windows | High |
| R3 | Keycloak | LOGIN on a registry or gateway client with no MFA flag in details | Stateless | Medium |
| R4 | Kubernetes | ClusterRoleBinding create/patch where roleRef.name \== cluster-admin | Stateless | Critical |
| R5 | Kubernetes | get on a Secret where requester namespace ≠ secret namespace | Stateless | High |
| R6 | Kubernetes | Pod create/update in a model-serving namespace with an image not from the approved ECR prefix | Allow-list | High |
| R7 | Pipeline | DAG\_RUN trigger type \= manual for a training or prod DAG, outside its schedule | DAG metadata table | Medium |
| R8 | Pipeline | OpenLineage run reads an input dataset with no owner | KTable dataset\_owners | High |
| R9 | Pipeline | DRIFT\_METRIC PSI \> 0.10 (medium) or \> 0.25 (high), or KS p \< 0.05 | Per model and feature | Medium to High |
| R10 | Pipeline | BIAS\_METRIC demographic-parity gap \> 0.10, or disparate-impact ratio \< 0.80 | Per model and protected attribute | High |
| R11 | Model registry | Stage changes to Production while approval\_status \!= Approved | KTable model\_approvals | Critical |
| R12 | Model registry | registered\_by \== approved\_by for the same model version | KTable join | High |
| R13 | Model registry | Register event missing owner, dataset ID or eval metrics | Stateless | Medium |
| R14 | LLM gateway | PII entity found in prompt or response (email, phone, ID numbers, names) | Stateless, run before storage | High to Critical |
| R15 | LLM gateway | Inference record missing model version, request ID or user ID | Stateless | Low |
| R16 | LLM gateway | model\_id not in the approved-models table | Allow-list | High |

Check order in the engine

1. Validate the schema. Send malformed events to a dead-letter topic.  
2. Run the stateless rules.  
3. Run the windowed rules.  
4. Run the stateful joins (R8, R11, R12).  
5. Emit one ComplianceEvent per rule hit, carrying the rule ID, severity, mapped frameworks, evidence and event\_id.

### 2\. How risk scoring works

Stage A: rule-based baseline

* Event score: S × C × F × B  
  * S \= severity weight (Low 1, Medium 3, High 6, Critical 10\)  
  * C \= asset criticality from tags (prod 1.5, staging 1.0, dev 0.5)  
  * F \= repetition factor, 1 \+ ln(1 \+ n), where n is earlier hits of the same rule and entity in 24 h  
  * B \= framework breadth, 1 \+ 0.1 × (frameworks hit − 1\)  
* Entity score (per actor, model, namespace or dataset): Σ event\_score × exp(−Δt/τ) with τ \= 6 h, so old hits fade.  
* Normalise to 0–100: risk \= 100 × (1 − exp(−raw/K)), with K tuned so one critical production event scores about 70\.  
* Bands: 0–24 Low, 25–49 Medium, 50–74 High, 75–100 Critical.

Stage B: ML anomaly score (Isolation Forest)

* Kafka Streams emits a feature vector per entity every 5 minutes to a risk-features topic.  
  * Event counts by type  
  * Distinct source IPs and resources touched  
  * Off-hours ratio  
  * Error ratio  
  * Rule-hit count  
  * PII-hit rate  
  * Drift and bias values  
* Train on the clean baseline days only.  
* A Python scorer on EKS reads risk-features, applies the forest, and writes an anomaly score a ∈ \[0,1\]. Retrain daily.  
* Final score: 0.7 × rule\_risk \+ 0.3 × (100 × a). Tune the weights on validation data.

Why two stages: The rules explain why something fired, which auditors need. The forest catches unusual behaviour no rule covers. The evaluation compares baseline against hybrid using PR-AUC and the false-positive rate.

### 3\. Methodology in depth

Phase 1: event model

* One envelope for every producer: event\_id, source, event\_type, actor, resource, env, event\_time, payload.  
* event\_id is deterministic (a hash of the source and its native ID), so retries can be de-duplicated.  
* Register schemas in AWS Glue Schema Registry.

Phase 2: infrastructure

* Terraform builds the VPC across 2 AZs, private subnets, NAT, security groups, KMS keys, and one IRSA role per workload.  
* MSK uses TLS plus SASL/IAM, 3 brokers, replication factor 3 and 6 partitions per topic.  
* Raw topics have 30-day retention. The dataset-owner and model-approval topics are compacted so they can back KTables.

Phase 3: ingestion

* Producers use at-least-once delivery with idempotent keys.  
* Partition key is actor or resource, so related events stay in order.  
* Keycloak uses its event-listener SPI. Kubernetes audit uses Fluent Bit or a CloudWatch Lambda. Airflow uses a listener plus the OpenLineage Kafka transport. SageMaker uses an EventBridge rule and Lambda. Bedrock uses invocation logs and Lambda.  
* The drift/bias DAG publishes DRIFT\_METRIC and BIAS\_METRIC events.

Phase 4: stream processing

* Kafka Streams runs on EKS with 3 replicas and exactly\_once\_v2.  
* Processing uses event time, 1-minute grace, and a late-event side output.  
* State stores are backed by changelog topics, so a pod restart rebuilds state.  
* PII detection (regex plus Presidio or Comprehend) runs before anything is persisted. Redact matched spans in the stored copy and keep only the evidence reference.

Phase 5: compliance mapping

* A versioned table maps each rule\_id to framework and control IDs for the 5 frameworks.  
* Update the table without redeploying, so a control change does not need a code change.

Phase 6: persistence and serving

* A sink writes compliance\_events, audit\_summary and risk\_scores to RDS with event\_id as a unique key, so duplicates are ignored.  
* A Postgres view unnests frameworks\[\] for Tableau.  
* Tableau uses a live connection. Sheets: framework status, risk trend, incident history, drift and bias, pipeline health.

Phase 7: evaluation

* Detection quality:  
  * The injector writes a ground-truth row per scenario with expected rule\_id and time.  
  * A hit is a true positive if the same rule fires within T seconds.  
  * Report precision, recall and F1 per rule.  
* Latency: detected\_at − event\_time and ingested\_at → detected\_at, as p50/p95/p99.  
* Throughput: replay at 1k, 5k and 10k events/s and record the maximum sustained rate.  
* Resilience: kill a stream pod and a broker, compare offsets and counts, and confirm no loss or duplicates.  
* Risk scoring: baseline vs hybrid, plus an ablation with no recency decay.

Phase 8: operations and security

* Encryption at rest and in transit, least-privilege IAM, and no PII in the stored evidence.  
* CloudWatch dashboards and alarms: consumer lag, error rate, DLQ depth.  
* Terraform destroy after each session to control cost.

### 4\. Real data stats and links

No public dataset contains governance events from these five producers, to my knowledge. Your pipeline's events must be generated, as already planned. Use real public data for the parts it does fit.

Loghub (log anomaly baseline for the Isolation Forest)

* Link: [https\://github.com/logpai/loghub](https://github.com/logpai/loghub) (paper: [https\://arxiv.org/abs/2008.06448](https://arxiv.org/abs/2008.06448))  
* HDFS covers 38.7 hours with 11,175,629 messages (1.47 GB). BGL covers 214.7 days with 4,747,963 messages (708.76 MB). Both are labeled. [arxiv](https://arxiv.org/pdf/2008.06448)  
*   
* Loghub has 19 datasets, of which 6 are labeled. [arxiv](https://arxiv.org/pdf/2008.06448v2)  
*   
* BGL project page: [https\://www\.usenix.org/cfdr-data\#hpc4](https://www.usenix.org/cfdr-data#hpc4)  
* Anomaly counts are often quoted in the literature as 16,838 anomalous of 575,061 HDFS blocks, and 348,460 BGL alert messages. I didn't confirm these in this session, so check them on the dataset pages before you use them.

UCI Adult (bias metrics for R10, and a model to register)

* Link: [https\://archive.ics.uci.edu/dataset/2/adult](https://archive.ics.uci.edu/dataset/2/adult)  
* It has 48,842 instances and 14 features, includes missing values, and is licensed CC BY 4.0. [uci](https://archive.ics.uci.edu/dataset/2/adult.)  
*   
* About 23.93% of records are \>50K and 76.07% are ≤50K. [deepchecks](https://docs.deepchecks.com/0.13/api/generated/deepchecks.tabular.datasets.classification.adult.html)  
*   
* 45,222 instances have no missing values. [ugr](https://sci2s.ugr.es/keel/dataset.php?cod=192)  
*   
* Use sex and race as the protected attributes for demographic-parity and disparate-impact checks.

How to use them in the deck

* EDA slide: show the real Loghub and Adult statistics above as "supporting datasets".  
* Report your generated corpus separately, with its own counts, so the two are never mixed.  
* Say explicitly that the governance events are synthetic and injected, since reviewers will ask.

I can write the injection scripts, the risk-scoring code or the Kafka Streams rule skeleton next if you tell me which to start with.

no give me real stats and sites , and source telling real ai governance and compliance in real world ,, some rbeach that happpend , some violations etc

then tell me how i should simulate the 5 producers to show during demo the various lpg and detections

Searched the web, updated memory

## Part 1: Real-world evidence (use on the Introduction and Motivation slides)

### A. Industry statistics (IBM / Ponemon, Cost of a Data Breach)

Primary page: [https\://www\.ibm.com/reports/data-breach](https://www.ibm.com/reports/data-breach)

| Metric | 2025 report | 2026 report |
| ----- | ----- | ----- |
| Global average breach cost | \$4.44M | \$4.99M (up 12%) |
| AI-related breach victims lacking AI access controls | 97% | 92% |
| Breached orgs lacking AI governance policies | 63% | 68% |
| Share of breaches involving AI systems | 13% | 21% |
| Shadow AI involvement | 20% of orgs reported shadow-AI breaches; high shadow AI added \$670K | 43%, more than double |
| AI-driven attacks | n/a | up 56%, about \$1M added per breach [2025 Cost of a Data Breach Report: Navigating the AI rush without sidelining security \+10](https://www.ibm.com/think/x-force/2025-cost-of-a-data-breach-navigating-ai) |

* Model inversion breaches averaged \$6.07M and prompt injection breaches \$5.89M. [cycode](https://cycode.com/blog/ibm-cost-of-data-breach-2026/)  
*   
* Only 40% of organizations reported using access controls on AI models and data. [ibm](https://www.ibm.com/think/x-force/2026-cost-of-a-data-breach-ai-adversaries-enterprise-risk)  
*   
* Most 2026 numbers come from law-firm and security-blog summaries of the IBM report. Cite the IBM page itself on your slide and check the figures there first.

### B. Real incidents and enforcement cases

| Case | What happened | Maps to your rule |
| ----- | ----- | ----- |
| Samsung, 2023 | Engineers put confidential source code and meeting notes into ChatGPT, with three leak instances within a month, and the company then banned generative AI tools. | R14 PII/secrets in prompts, R16 shadow AI |
| Microsoft AI research, 2023 | An over-permissive Azure SAS token meant to share training data exposed an entire storage account, 38 TB including workstation backups, keys and 30,000+ Teams messages. It had been public since July 2020\. | R5, R8, R13 (data and secrets exposure, no owner or review). Source: [https\://www\.wiz.io/blog/38-terabytes-of-private-data-accidentally-exposed-by-microsoft-ai-researchers](https://www.wiz.io/blog/38-terabytes-of-private-data-accidentally-exposed-by-microsoft-ai-researchers) |
| OpenAI, Italy Garante, Dec 2024 | €15M fine for training on personal data without an adequate legal basis. It also cited a breach that was not notified to the authority. | R14, R15, GDPR Art. 5/30/32 |
| Same case, 2026 outcome | A Rome court annulled the fine on 18 March 2026 on a single jurisdictional point, and not because the practices were ruled lawful. | Present it as annulled on procedure |
| Clearview AI (FR, IT, GR 2022; NL 2024\) | €20M fines each from France, Italy and Greece, and €30.5M from the Dutch DPA for building a facial-image database without legal basis. | GDPR data-governance rules (R8) |
| Replika / Luka, Italy | €5M fine from the Garante for AI companion chatbot privacy violations. | R14, transparency [223 samsung ban use generative ai services employees \+9](https://www.medianama.com/2023/05/223-samsung-ban-use-generative-ai-services-employees/) |

The Clearview and Replika rows come from secondary trackers. Confirm them against the regulators' own decisions (for example [https\://www\.garanteprivacy.it](https://www.garanteprivacy.it/)) before you submit.

### C. Regulation status as of October 2026 (this changes your mapping)

* The Digital Omnibus is now Regulation (EU) 2026/1744 and has been in force since 27 July 2026\. [usercentrics](https://usercentrics.com/knowledge-hub/eu-ai-act-high-risk-delay-article-50-transparency-consent/)  
*   
* It moves Annex III high-risk obligations to 2 December 2027 and Annex I to 2 August 2028\. [usercentrics](https://usercentrics.com/knowledge-hub/eu-ai-act-high-risk-delay-article-50-transparency-consent/)  
*   
* From 2 August 2026 the AI Office can enforce general-purpose AI rules, and chatbot, deepfake and AI-content transparency duties apply. [corp-intl](https://corp-intl.com/news/what-is-the-timeline-for-implementing-the-eu-ai-act)  
*   
* Fines reach up to €35M or 7% of turnover. Non-compliance with high-risk obligations carries up to €15M or 3%. [felloai](https://felloai.com/eu-ai-act/)  
* [aiacto](https://www.aiacto.eu/en/blog/ai-act-what-changes-august-2-2026)  
*   
* One tracker reported no public AI Act penalties issued yet. I only saw this in a secondary source, so verify it before stating it. [axis-intelligence](https://axis-intelligence.com/eu-ai-act-news/)  
*   
* Fix to my earlier table: The AI Act articles I mapped (Art. 9, 10, 11, 12, 14, 15\) are high-risk obligations, so they bind from December 2027\. Label them "readiness mapping" and state the date on the slide.  
* Primary source: [https\://eur-lex.europa.eu/eli/reg/2024/1689/oj](https://eur-lex.europa.eu/eli/reg/2024/1689/oj)

### D. Where to find more real cases

* AI Incident Database: [https\://incidentdatabase.ai](https://incidentdatabase.ai/)  
* OECD AI Incidents Monitor: [https\://oecd.ai/en/incidents](https://oecd.ai/en/incidents)  
* GDPR Enforcement Tracker (CMS): [https\://www\.enforcementtracker.com](https://www.enforcementtracker.com/)  
* I recalled these three sites from memory and didn't open them this session, so check that they load.

---

## Part 2: Simulating the 5 producers for the demo

### Strategy

* Hybrid: Use real components where it is cheap and synthetic emitters where it is costly. Every simulator writes the same envelope into the same Kafka topics, so the pipeline can't tell them apart.  
* Real: Keycloak (Docker) and Kubernetes (kind or EKS) produce real logs from simple commands.  
* Synthetic: model registry, LLM gateway and the Airflow drift/bias events. Real SageMaker, Bedrock and MWAA add cost and live-demo risk for little extra credibility.  
* Fallback: Record a full run to JSONL and keep a replay command, so the demo works if the network fails.

### Components

1. Background traffic generator: emits normal events at a steady rate, for example 20 to 50 per second across the 5 topics (Poisson timing, Faker users and models).  
2. Scenario driver: a small CLI or Streamlit page with one button per scenario that injects labeled violations and writes scenario\_id to a ground-truth file.  
3. Three screens during the demo: (a) a terminal tailing the raw topic for the current scenario, (b) a terminal tailing compliance-events, (c) the Tableau or Grafana board.

### Per-producer simulation

| Producer | Real or synthetic | Normal traffic | Violation to inject | Rule |
| ----- | ----- | ----- | ----- | ----- |
| Keycloak | Real (Docker, realm demo) | Successful logins via the token endpoint | 6 wrong-password token requests in 1 min; grant a user the model-approver role at night | R1, R2 |
| Kubernetes | Real (kind with an audit policy) | kubectl get pods, deployments | kubectl create clusterrolebinding mallory-admin \--clusterrole=cluster-admin \--user=mallory; read a Secret across namespaces; deploy a pod from docker.io | R4, R5, R6 |
| Pipeline | Synthetic (or Airflow in Docker) | Scheduled run events and lineage events with dataset owners | Manual trigger of the training DAG; read a dataset with owner: null; DRIFT\_METRIC PSI 0.31; BIAS\_METRIC gap 0.18 | R7, R8, R9, R10 |
| Model registry | Synthetic (SageMaker-style event) | Register, then Approve, then promote to Production | Promote with PendingManualApproval; same user registers and approves; register with no owner | R11, R12, R13 |
| LLM gateway | Synthetic (Bedrock-style record) | Clean prompts to approved models | Prompt containing an email and phone number; call to an unapproved model; record missing request\_id | R14, R15, R16 |

Sample events (one JSON per line, all share the envelope)  
json  
{"event\_id":"kc-001","source":"keycloak","event\_type":"LOGIN\_ERROR","actor":"alice","resource":"client:registry-admin","env":"prod","event\_time":"2026-10-04T10:00:01Z","payload":{"ip":"10.0.3.7","error":"invalid\_user\_credentials"}}  
{"event\_id":"mr-014","source":"model-registry","event\_type":"MODEL\_STAGE\_CHANGE","actor":"dev-bob","resource":"model:churn-v7","env":"prod","event\_time":"2026-10-04T10:05:12Z","payload":{"to\_stage":"Production","approval\_status":"PendingManualApproval","owner":"ml-team"}}  
{"event\_id":"lg-203","source":"llm-gateway","event\_type":"INFERENCE","actor":"svc-chatbot","resource":"model:unapproved-llm-x","env":"prod","event\_time":"2026-10-04T10:07:40Z","payload":{"prompt":"My email is a@b.com and phone 98xxxxxx12","request\_id":null}}  
{"event\_id":"pl-088","source":"pipeline","event\_type":"DRIFT\_METRIC","actor":"airflow","resource":"model:churn-v7","env":"prod","event\_time":"2026-10-04T10:09:03Z","payload":{"feature":"age","metric":"PSI","value":0.31,"threshold":0.25}}  
{"event\_id":"k8-519","source":"k8s-audit","event\_type":"RBAC\_BINDING","actor":"mallory","resource":"clusterrolebinding/mallory-admin","env":"prod","event\_time":"2026-10-04T10:11:55Z","payload":{"role":"cluster-admin"}}

Emitter skeleton (Python, pip install confluent-kafka faker)  
python  
import json, time, random, uuid, sys  
from datetime import datetime, timezone  
from confluent\_kafka import Producer  
p \= Producer({"bootstrap.servers": "localhost:9092"})  \# MSK: add SASL/IAM config

def emit(topic, source, etype, actor, resource, payload, env="prod"):  
    ev \= {"event\_id": str(uuid.uuid4()), "source": source, "event\_type": etype,  
          "actor": actor, "resource": resource, "env": env,  
          "event\_time": datetime.now(timezone.utc).isoformat(), "payload": payload}  
    p.produce(topic, json.dumps(ev).encode(), key=actor.encode()); p.poll(0)  
    return ev\["event\_id"\]

def log\_truth(scenario, rule, event\_ids):  \# ground truth for precision/recall  
    with open("truth.jsonl", "a") as f:  
        f.write(json.dumps({"scenario": scenario, "rule": rule, "ids": event\_ids,  
                            "t": time.time()}) \+ "\\n")

def s\_unapproved\_promotion():            \# R11  
    i \= emit("model-registry-events", "model-registry", "MODEL\_STAGE\_CHANGE",  
             "dev-bob", "model:churn-v7", {"to\_stage": "Production",  
             "approval\_status": "PendingManualApproval", "owner": "ml-team"})  
    log\_truth("unapproved\_promotion", "R11", \[i\])

def s\_pii\_prompt():                      \# R14  
    i \= emit("llm-gateway-events", "llm-gateway", "INFERENCE", "svc-chatbot",  
             "model:approved-llm", {"prompt": "Mail me at a@b.com, ph 9812345678",  
             "request\_id": str(uuid.uuid4())})  
    log\_truth("pii\_prompt", "R14", \[i\])

SCENARIOS \= {"promote": s\_unapproved\_promotion, "pii": s\_pii\_prompt}  
if \_\_name\_\_ \== "\_\_main\_\_":  
    SCENARIOS\[sys.argv\[1\]\](); p.flush()

Add one function per rule in the same shape, and a separate baseline.py loop for normal traffic.

Real Keycloak failed logins (R1):  
bash  
for i in 1 2 3 4 5 6; do curl \-s \-o /dev/null \-d "client\_id=registry-admin\&username=alice\&password=wrong\$i\&grant\_type=password" \\  
  http\://localhost:8080/realms/demo/protocol/openid-connect/token; done

### Demo script (about 10 minutes)

| Min | Step | What the audience sees |
| ----- | ----- | ----- |
| 0–1 | Show architecture and the 5 topics | Slide, plus kafka-topics \--list |
| 1–3 | Start baseline traffic | Raw events scrolling, dashboard near zero risk |
| 3–6 | Press 4 or 5 single scenarios (R1, R11, R14, R9) | Raw event, then a compliance-events row with mapped frameworks, then a dashboard tile and risk jump |
| 6–8 | Run the attack story: brute-force login (R1), role grant (R2), unapproved promotion (R11), PII prompts (R14) | One actor's entity risk climbing from Low to Critical |
| 8–9 | Show results from truth.jsonl | Precision, recall and p95 latency table |
| 9–10 | Kill a stream-processor pod | Rebalance, with no lost events |

Making detections visible

* Print each hit in the processor log as one line: RULE=R11 SEV=critical ACTOR=dev-bob FRAMEWORKS=\[AI RMF, ISO 42001, EU AI Act Art.14, 800-53 CM-3\] RISK=82.  
* Keep the dashboard on a live connection with a 5-second refresh.  
* Rehearse the order and keep the replay fallback ready.

I can write the full baseline generator and all 16 scenario functions next, or the Keycloak and kind setup for the two real producers, if you tell me which first.

