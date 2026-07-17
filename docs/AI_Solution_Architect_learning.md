# AI Solution Architect - Interview Preparation Guide

## Based on Population Advyzer Codebase Context

---

# Table of Contents

| # | Topic | Page |
|---|-------|------|
| 1 | [Azure AI Foundry & Agentic AI](#question-1-design-and-implement-agentic-ai-solutions-using-microsoft-azure-ai-foundry-for-enterprise-scale-deployments) | Designing enterprise-scale agentic solutions |
| 2 | [Multi-Agent Decision Frameworks](#question-2-architect-multi-agent-decision-frameworks-langgraph-supporting-hierarchical-peer-to-peer-and-pipeline-topologies) | Hierarchical, Peer-to-Peer, Pipeline topologies |
| 3 | [Fault-Tolerant Agent Systems](#question-3-engineer-fault-tolerant-agent-systems-with-end-to-end-observability-monitoring-and-self-healing-capabilities) | Observability, monitoring, self-healing |
| 4 | [Workflow Management Systems](#question-4-design-workflow-management-systems-governing-agent-and-node-interactions-sequencing-and-state-management) | Sequencing, state management, DAGs |
| 5 | [Meta-Agent Hierarchies](#question-5-architect-meta-agent-agents-of-agents-hierarchies-for-complex-layered-autonomous-decision-making) | Agents of Agents for layered decision-making |
| 6 | [A2A Protocol Standards](#question-6-implement-a2a-agent-to-agent-protocol-standards-for-secure-structured-inter-agent-communication) | Secure inter-agent communication |
| 7 | [Agentic Framework Selection](#question-7-assess-and-select-agentic-frameworks-langgraph-langchain-autogen-semantic-kernel-based-on-use-case-fit) | LangGraph, LangChain, AutoGen, Semantic Kernel |
| 8 | [MCP Hub Architecture](#question-8-govern-mcp-hub-architecture-defining-policies-and-standards-across-a-centralized-pool-of-mcp-servers) | Policies and standards governance |
| 9 | [MCP Server Boundaries](#question-9-define-mcp-server-boundaries-responsibilities-and-segregation-strategies-within-the-enterprise-hub) | Responsibilities and segregation |
| 10 | [MCP vs Azure APIM](#question-10-advise-decision-criteria-for-mcp-vs-azure-apim-based-on-integration-patterns-and-governance-needs) | Decision criteria for integration patterns |
| 11 | [OCR Solution Evaluation](#question-11-evaluate-and-select-ocr-solutions-azure-document-intelligence-vs-john-snow-labs-aligned-to-accuracy-and-scale-requirements) | Azure Doc Intelligence vs John Snow Labs |
| 12 | [OCR Pipeline Design](#question-12-design-and-optimize-ocr-pre-processing-and-post-processing-pipelines-for-healthcare-document-ingestion) | Pre/post-processing for healthcare docs |
| 13 | [RAG vs CAG Strategies](#question-13-architect-and-recommend-between-rag-retrieval-augmented-generation-and-cag-cache-augmented-generation-strategies-based-on-use-case-analysis) | Retrieval vs Cache-Augmented Generation |
| A | [End-to-End Architecture](#end-to-end-architecture-databricks--snowflake--tableau) | Databricks → Snowflake → Tableau |
| B | [Population Advyzer System Architecture](#appendix-b-population-advyzer-system-architecture) | Risk Engine & Gap Suspecting detailed flows |

---

# Question 1: Design and implement Agentic AI solutions using Microsoft Azure AI Foundry for enterprise-scale deployments

## Current State vs. Agentic AI Future

The **Population Advyzer** is a traditional **rule-based data engineering platform**. This guide shows how to articulate transforming it into an **Agentic AI system**.

---

## 1. What is Azure AI Foundry?

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                              AZURE AI FOUNDRY                                            │
│                    (Unified Platform for Building AI Agents)                             │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐ │
│  │  MODEL CATALOG  │   │  PROMPT FLOW    │   │  AGENT SERVICE  │   │  EVALUATION     │ │
│  │                 │   │                 │   │                 │   │                 │ │
│  │ • GPT-4o        │   │ • Visual DAG    │   │ • Agent Runtime │   │ • Groundedness  │ │
│  │ • Claude        │   │ • Tool Binding  │   │ • Tool Registry │   │ • Relevance     │ │
│  │ • Llama         │   │ • RAG Patterns  │   │ • Memory Store  │   │ • Coherence     │ │
│  │ • Fine-tuned    │   │ • Deployments   │   │ • Orchestration │   │ • Safety        │ │
│  └─────────────────┘   └─────────────────┘   └─────────────────┘   └─────────────────┘ │
│                                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐│
│  │                           ENTERPRISE FEATURES                                        ││
│  │  • Azure OpenAI Service Integration    • Content Safety Filters                     ││
│  │  • Private Endpoints / VNet            • Role-Based Access Control                  ││
│  │  • Managed Identity                    • Audit Logging & Compliance                 ││
│  │  • MLOps / LLMOps Pipelines           • Responsible AI Dashboard                   ││
│  └─────────────────────────────────────────────────────────────────────────────────────┘│
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. What is "Agentic AI"?

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         TRADITIONAL vs. AGENTIC AI                                       │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   TRADITIONAL (Current Pop Advyzer)         AGENTIC AI (Future State)                   │
│   ─────────────────────────────────         ─────────────────────────                   │
│                                                                                          │
│   ┌─────────────────────────────┐           ┌─────────────────────────────┐            │
│   │  RULE-BASED PIPELINE        │           │  AUTONOMOUS AGENTS          │            │
│   │                             │           │                             │            │
│   │  IF icd_code IN hcc_map     │           │  "Analyze this claim and    │            │
│   │    THEN assign_hcc()        │           │   determine the most likely │            │
│   │                             │           │   HCC based on clinical     │            │
│   │  IF parent_hcc EXISTS       │           │   context, not just codes"  │            │
│   │    THEN suppress_child()    │           │                             │            │
│   └─────────────────────────────┘           └─────────────────────────────┘            │
│                                                                                          │
│   • Deterministic                           • Probabilistic + Reasoning                 │
│   • Pre-defined rules                       • Goal-directed behavior                    │
│   • No learning                             • Learns from feedback                      │
│   • Brittle to edge cases                   • Handles ambiguity                         │
│   • Human codes all logic                   • Agent plans & executes                    │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**Agentic AI Key Characteristics:**
1. **Autonomy** - Makes decisions without step-by-step human instruction
2. **Goal-Oriented** - Given a goal, plans its own approach
3. **Tool Use** - Calls APIs, databases, other agents as needed
4. **Memory** - Remembers context across interactions
5. **Reasoning** - Explains its decisions (chain-of-thought)

---

## 3. How to Transform Population Advyzer into Agentic AI

### Architecture Vision

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    AGENTIC POPULATION ADVYZER (Azure AI Foundry)                         │
└──────────────────────────────────────────────────────────────────────────────────────────┘

                              ┌─────────────────────────────┐
                              │     ORCHESTRATOR AGENT      │
                              │   (Meta-Agent / Supervisor) │
                              │                             │
                              │  "Process risk adjustment   │
                              │   for Plan XYZ, Q4 2026"    │
                              └──────────────┬──────────────┘
                                             │
                    ┌────────────────────────┼────────────────────────┐
                    │                        │                        │
                    ▼                        ▼                        ▼
    ┌───────────────────────┐  ┌───────────────────────┐  ┌───────────────────────┐
    │   DATA QUALITY AGENT  │  │   RISK SCORING AGENT  │  │   GAP ANALYSIS AGENT  │
    │                       │  │                       │  │                       │
    │ • Validate claims     │  │ • Interpret diagnoses │  │ • Find missing HCCs   │
    │ • Detect anomalies    │  │ • Apply clinical logic│  │ • Reason about gaps   │
    │ • Fix data issues     │  │ • Handle edge cases   │  │ • Prioritize suspects │
    │ • Report findings     │  │ • Explain scores      │  │ • Generate evidence   │
    └───────────┬───────────┘  └───────────┬───────────┘  └───────────┬───────────┘
                │                          │                          │
                │                          │                          │
    ┌───────────▼───────────────────────────▼──────────────────────────▼───────────┐
    │                              TOOL REGISTRY                                    │
    │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │
    │  │ SQL Tool │ │ ICD API  │ │ CMS Refs │ │ Delta    │ │ Clinical │           │
    │  │          │ │          │ │          │ │ Tables   │ │ KB (RAG) │           │
    │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘           │
    └──────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Concrete Agent Examples for Population Advyzer

### Agent 1: Clinical Gap Reasoning Agent

**Current State (Rule-Based):**
```python
# Current gap_suspecting_helper.py - rigid rules
if ndc_code in method_metadata['CLAIM_CD']:
    create_gap()  # No clinical reasoning
```

**Future State (Agentic AI):**
```python
# Azure AI Foundry Agent with clinical reasoning
class ClinicalGapAgent:
    """
    Agent that reasons about potential HCC gaps using clinical knowledge.
    """
    
    system_prompt = """
    You are a clinical coding expert specializing in CMS-HCC risk adjustment.
    
    Given a member's claims history, pharmacy data, and lab results:
    1. Identify potential undocumented chronic conditions
    2. Provide clinical reasoning for each suspected gap
    3. Estimate confidence based on evidence strength
    4. Suggest what additional documentation would confirm the gap
    
    Use the tools available to query claims, lookup ICD codes, and check CMS guidelines.
    """
    
    tools = [
        query_claims_tool,      # SQL query against medical_claims
        icd_lookup_tool,        # ICD-10 code descriptions
        hcc_mapping_tool,       # ICD → HCC mappings
        clinical_guidelines_rag # RAG over CMS/clinical guidelines
    ]
    
    async def analyze_member(self, member_bid: str) -> GapAnalysis:
        """Agent autonomously plans and executes gap analysis"""
        
        # Agent decides what to query based on goal
        result = await self.run(
            goal=f"Identify potential HCC gaps for member {member_bid}",
            context={"risk_year": 2026, "plan": "BCBSAR"}
        )
        return result
```

**Agent Output Example:**
```json
{
  "member_bid": "1100000000000123",
  "suspected_gaps": [
    {
      "hcc_code": "HCC019",
      "hcc_description": "Diabetes with Chronic Complications",
      "confidence": 0.87,
      "reasoning": "Member has consistent Metformin fills (NDC 00378-1902) for 18 months, 
                   recent A1C lab value of 8.2%, and retinal exam claim. However, 
                   no DM diagnosis code captured on recent claims. High likelihood 
                   of undocumented diabetic complications.",
      "evidence": [
        {"type": "pharmacy", "detail": "Metformin 1000mg, 18 consecutive months"},
        {"type": "lab", "detail": "A1C = 8.2% on 2026-03-15"},
        {"type": "procedure", "detail": "CPT 92012 - Retinal exam on 2026-02-20"}
      ],
      "suggested_action": "Request medical records from PCP visit on 2026-01-10"
    }
  ]
}
```

---

### Agent 2: HCC Hierarchy Reasoning Agent

**Current State:**
```python
# Rigid parent-child suppression
if parent_hcc in member_hccs and child_hcc in member_hccs:
    suppress(child_hcc)  # No reasoning about clinical validity
```

**Future State (Agentic):**
```python
class HierarchyReasoningAgent:
    """
    Agent that applies HCC hierarchy with clinical judgment.
    """
    
    system_prompt = """
    You are an expert in CMS-HCC hierarchy rules.
    
    When evaluating hierarchy suppression:
    1. Verify the parent-child relationship is clinically appropriate
    2. Check if the child condition might be a distinct comorbidity
    3. Flag cases where automatic suppression may lose valid RAF
    4. Explain your reasoning for audit purposes
    """
    
    async def evaluate_suppression(self, member_hccs: List[str]) -> SuppressionResult:
        # Agent reasons about each potential suppression
        pass
```

---

### Agent 3: Data Quality Agent

```python
class DataQualityAgent:
    """
    Autonomous agent that monitors and fixes data quality issues.
    """
    
    system_prompt = """
    You are a healthcare data quality specialist.
    
    Monitor incoming claims for:
    1. Invalid ICD-10 codes (not in CMS reference)
    2. Impossible date combinations (service date > paid date)
    3. Duplicate claims
    4. Missing required fields
    5. Outlier patterns suggesting data entry errors
    
    For each issue:
    - Classify severity (Critical/Warning/Info)
    - Attempt automatic correction if possible
    - Log for human review if uncertain
    """
    
    tools = [
        validate_icd_tool,
        check_date_logic_tool,
        detect_duplicates_tool,
        auto_correct_tool,
        alert_human_tool
    ]
```

---

## 5. Azure AI Foundry Implementation Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                     AZURE AI FOUNDRY DEPLOYMENT ARCHITECTURE                             │
└──────────────────────────────────────────────────────────────────────────────────────────┘

    ┌─────────────────────────────────────────────────────────────────────────────────────┐
    │                              AZURE AI FOUNDRY PROJECT                                │
    │                                                                                      │
    │  ┌─────────────────────────────────────────────────────────────────────────────┐   │
    │  │                           AGENT DEFINITIONS                                  │   │
    │  │                                                                              │   │
    │  │   gap_analysis_agent.yaml          risk_scoring_agent.yaml                  │   │
    │  │   ┌─────────────────────┐          ┌─────────────────────┐                  │   │
    │  │   │ model: gpt-4o       │          │ model: gpt-4o       │                  │   │
    │  │   │ temperature: 0.1    │          │ temperature: 0.0    │                  │   │
    │  │   │ tools:              │          │ tools:              │                  │   │
    │  │   │   - sql_query       │          │   - icd_hcc_lookup  │                  │   │
    │  │   │   - clinical_rag    │          │   - coefficient_calc│                  │   │
    │  │   │   - pharmacy_lookup │          │   - hierarchy_check │                  │   │
    │  │   └─────────────────────┘          └─────────────────────┘                  │   │
    │  └─────────────────────────────────────────────────────────────────────────────┘   │
    │                                                                                      │
    │  ┌─────────────────────────────────────────────────────────────────────────────┐   │
    │  │                              TOOL CONNECTIONS                                │   │
    │  │                                                                              │   │
    │  │   ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐     │   │
    │  │   │ Databricks SQL   │    │ Azure AI Search  │    │ Azure Functions  │     │   │
    │  │   │ (Unity Catalog)  │    │ (Clinical RAG)   │    │ (Custom Tools)   │     │   │
    │  │   │                  │    │                  │    │                  │     │   │
    │  │   │ pop_stg.curation │    │ cms_guidelines   │    │ icd_validator()  │     │   │
    │  │   │ .risk_member_hcc │    │ clinical_papers  │    │ ndc_lookup()     │     │   │
    │  │   └──────────────────┘    └──────────────────┘    └──────────────────┘     │   │
    │  └─────────────────────────────────────────────────────────────────────────────┘   │
    │                                                                                      │
    │  ┌─────────────────────────────────────────────────────────────────────────────┐   │
    │  │                           PROMPT FLOW (Orchestration)                        │   │
    │  │                                                                              │   │
    │  │   ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐  │   │
    │  │   │ Ingest  │───▶│ Validate│───▶│ Score   │───▶│ Analyze │───▶│ Output  │  │   │
    │  │   │ Claims  │    │ Quality │    │ Risk    │    │ Gaps    │    │ Results │  │   │
    │  │   └─────────┘    └─────────┘    └─────────┘    └─────────┘    └─────────┘  │   │
    │  └─────────────────────────────────────────────────────────────────────────────┘   │
    │                                                                                      │
    └──────────────────────────────────────────────────────────────────────────────────────┘
                                             │
                                             ▼
    ┌─────────────────────────────────────────────────────────────────────────────────────┐
    │                              ENTERPRISE INTEGRATION                                  │
    │                                                                                      │
    │  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐              │
    │  │ Azure Event Grid │    │ Azure Monitor    │    │ Azure Key Vault  │              │
    │  │ (Triggers)       │    │ (Observability)  │    │ (Secrets)        │              │
    │  └──────────────────┘    └──────────────────┘    └──────────────────┘              │
    │                                                                                      │
    │  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐              │
    │  │ Private Endpoint │    │ Managed Identity │    │ HIPAA Compliance │              │
    │  │ (Network)        │    │ (Auth)           │    │ (PHI Protection) │              │
    │  └──────────────────┘    └──────────────────┘    └──────────────────┘              │
    │                                                                                      │
    └──────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Interview Answer Framework

When asked this question, structure your answer as:

### **1. Context (30 seconds)**
> "In my current role, I architected a Medicare Advantage risk adjustment platform processing millions of claims. While the current system uses rule-based PySpark pipelines, I've designed an evolution path to Agentic AI using Azure AI Foundry."

### **2. Technical Depth (2 minutes)**
> "Azure AI Foundry provides the enterprise foundation we need:
> 
> - **Agent Service** for deploying autonomous agents that can reason about clinical data, not just apply static rules
> - **Prompt Flow** for orchestrating multi-agent workflows where a supervisor agent delegates to specialized Gap Analysis, Risk Scoring, and Data Quality agents
> - **Tool Connections** that let agents query our Databricks Unity Catalog tables, call ICD-10 validation APIs, and use RAG over CMS guidelines
> - **Enterprise security** - Private endpoints, managed identity, and HIPAA compliance for PHI"

### **3. Concrete Example (1 minute)**
> "For example, our current gap suspecting logic uses rigid if-then rules - if NDC code matches, create gap. An Agentic approach would have a Clinical Gap Agent that:
> 
> 1. Analyzes the member's full claims history
> 2. Reasons about clinical likelihood ('18 months of Metformin + elevated A1C suggests diabetes')
> 3. Explains its confidence and evidence
> 4. Handles edge cases that break rule-based systems
> 
> This improves gap identification accuracy from ~75% to 90%+ while providing audit-ready explanations."

### **4. Scale & Operations (30 seconds)**
> "For enterprise scale, I would deploy agents as containerized services with autoscaling, implement circuit breakers for LLM API failures, use Azure Monitor for observability, and maintain human-in-the-loop review for high-stakes decisions given healthcare regulatory requirements."

---

## 7. Key Talking Points to Remember

| Topic | Your Answer |
|-------|-------------|
| **Why Agentic AI?** | Rule-based systems are brittle; agents handle ambiguity and edge cases |
| **Why Azure AI Foundry?** | Enterprise features (security, compliance, monitoring) + unified platform |
| **Healthcare Specifics** | HIPAA compliance, PHI protection, audit trails, explainability |
| **Your Experience** | "I've architected the rule-based version; I understand what agents need to replace" |
| **ROI Justification** | Better gap identification → higher RAF accuracy → more accurate risk scores |

---

## 8. Azure AI Foundry Components Deep Dive

### Model Catalog
- Access to Azure OpenAI models (GPT-4o, GPT-4 Turbo)
- Third-party models (Llama, Mistral, Cohere)
- Custom fine-tuned models
- Model benchmarking and comparison

### Agent Service
- Define agents with system prompts
- Register tools (functions, APIs, databases)
- Memory management (conversation, semantic)
- Multi-agent orchestration patterns

### Prompt Flow
- Visual workflow designer
- DAG-based orchestration
- Version control for prompts
- A/B testing deployments

### Evaluation Framework
- Groundedness (factual accuracy)
- Relevance (answer quality)
- Coherence (logical flow)
- Safety (harmful content detection)

---

## 9. Enterprise Deployment Checklist

```
□ Network Security
  □ Private endpoints for AI services
  □ VNet integration with Databricks
  □ Azure Firewall rules
  
□ Identity & Access
  □ Managed Identity for service-to-service
  □ RBAC for agent management
  □ Azure AD integration
  
□ Compliance
  □ HIPAA BAA with Azure
  □ PHI data residency
  □ Audit logging enabled
  □ Content safety filters
  
□ Operations
  □ Azure Monitor dashboards
  □ Alert rules for failures
  □ Cost management policies
  □ Backup/DR strategy
  
□ MLOps/LLMOps
  □ Prompt version control
  □ Model registry
  □ Evaluation pipelines
  □ Deployment automation
```

---

---

# Question 2: Architect Multi-Agent Decision Frameworks to orchestrate autonomous agent collaboration and goal resolution

---

## 1. What is a Multi-Agent Decision Framework?

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                        MULTI-AGENT DECISION FRAMEWORK                                    │
│                                                                                          │
│   A system where multiple specialized AI agents collaborate to solve complex problems    │
│   that no single agent could handle effectively alone.                                   │
│                                                                                          │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   KEY CONCEPTS:                                                                          │
│                                                                                          │
│   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐                       │
│   │  ORCHESTRATION  │   │  COLLABORATION  │   │ GOAL RESOLUTION │                       │
│   │                 │   │                 │   │                 │                       │
│   │ Who coordinates │   │ How agents      │   │ How complex     │                       │
│   │ the agents?     │   │ share info?     │   │ goals get done? │                       │
│   └─────────────────┘   └─────────────────┘   └─────────────────┘                       │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Multi-Agent Orchestration Patterns

### Pattern 1: Hierarchical (Supervisor)

```
                         ┌─────────────────────┐
                         │   SUPERVISOR AGENT  │
                         │   (Orchestrator)    │
                         │                     │
                         │ • Receives goal     │
                         │ • Delegates tasks   │
                         │ • Aggregates results│
                         └──────────┬──────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
              ▼                     ▼                     ▼
     ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
     │  AGENT A       │   │  AGENT B       │   │  AGENT C       │
     │  (Specialist)  │   │  (Specialist)  │   │  (Specialist)  │
     └────────────────┘   └────────────────┘   └────────────────┘

Best for: Clear task decomposition, controlled execution
```

### Pattern 2: Peer-to-Peer (Collaborative)

```
     ┌────────────────┐         ┌────────────────┐
     │   AGENT A      │◀───────▶│   AGENT B      │
     │                │         │                │
     └───────┬────────┘         └────────┬───────┘
             │                           │
             │    ┌────────────────┐     │
             └───▶│   AGENT C      │◀────┘
                  │                │
                  └────────────────┘

Best for: Complex reasoning, consensus building
```

### Pattern 3: Pipeline (Sequential)

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ AGENT A  │───▶│ AGENT B  │───▶│ AGENT C  │───▶│ AGENT D  │
│ (Ingest) │    │(Validate)│    │ (Score)  │    │ (Output) │
└──────────┘    └──────────┘    └──────────┘    └──────────┘

Best for: ETL workflows, staged processing
```

### Pattern 4: Debate/Adversarial

```
     ┌────────────────┐         ┌────────────────┐
     │   PROPOSER     │◀───────▶│   CRITIC       │
     │   AGENT        │ debate  │   AGENT        │
     └───────┬────────┘         └────────┬───────┘
             │                           │
             └───────────┬───────────────┘
                         ▼
                ┌────────────────┐
                │   JUDGE AGENT  │
                │   (Decision)   │
                └────────────────┘

Best for: High-stakes decisions, quality assurance
```

---

## 3. Population Advyzer Multi-Agent Architecture

### Current Monolithic Flow vs. Multi-Agent

```
CURRENT (Monolithic)                    FUTURE (Multi-Agent)
────────────────────                    ────────────────────

┌─────────────────────┐                 ┌─────────────────────────────────────┐
│                     │                 │         ORCHESTRATOR AGENT          │
│  Single Pipeline    │                 │  "Process Q4 2026 risk adjustment   │
│                     │                 │   for BCBSAR plan"                  │
│  data_loader        │                 └──────────────────┬──────────────────┘
│       ↓             │                                    │
│  transformation     │                 ┌──────────────────┼──────────────────┐
│       ↓             │                 │                  │                  │
│  qualifying_claims  │                 ▼                  ▼                  ▼
│       ↓             │        ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  risk_scoring       │        │ DATA QUALITY│    │RISK SCORING │    │GAP ANALYSIS │
│       ↓             │        │   AGENT     │    │   AGENT     │    │   AGENT     │
│  gap_suspecting     │        └─────────────┘    └─────────────┘    └─────────────┘
│                     │                 │                  │                  │
└─────────────────────┘                 └──────────────────┼──────────────────┘
                                                           │
                                        ┌──────────────────┼──────────────────┐
                                        │                  │                  │
                                        ▼                  ▼                  ▼
                                ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
                                │  CLINICAL   │    │  HIERARCHY  │    │ PERSISTENCE │
                                │  REASONING  │    │  REASONING  │    │   AGENT     │
                                │   AGENT     │    │   AGENT     │    │             │
                                └─────────────┘    └─────────────┘    └─────────────┘
```

---

## 4. Detailed Multi-Agent Design for Population Advyzer

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    POPULATION ADVYZER MULTI-AGENT FRAMEWORK                              │
└──────────────────────────────────────────────────────────────────────────────────────────┘

                    ┌─────────────────────────────────────────┐
                    │          ORCHESTRATOR AGENT             │
                    │                                         │
                    │  Goal: "Calculate risk scores and       │
                    │         identify gaps for Plan XYZ"     │
                    │                                         │
                    │  Responsibilities:                      │
                    │  • Decompose goal into subtasks         │
                    │  • Route to specialist agents           │
                    │  • Handle failures and retries          │
                    │  • Aggregate final results              │
                    └────────────────────┬────────────────────┘
                                         │
         ┌───────────────────────────────┼───────────────────────────────┐
         │                               │                               │
         ▼                               ▼                               ▼
┌─────────────────────┐      ┌─────────────────────┐      ┌─────────────────────┐
│  DATA QUALITY AGENT │      │  RISK SCORING AGENT │      │  GAP ANALYSIS AGENT │
│                     │      │                     │      │                     │
│  Subtasks:          │      │  Subtasks:          │      │  Subtasks:          │
│  • Validate claims  │      │  • Map ICD→HCC      │      │  • Method 1 (Rx)    │
│  • Check duplicates │      │  • Apply hierarchy  │      │  • Method 2 (Dx)    │
│  • Fix anomalies    │      │  • Calculate scores │      │  • Method 10 (Proc) │
│  • Report issues    │      │  • Normalize        │      │  • Cross-suppress   │
│                     │      │                     │      │                     │
│  Tools:             │      │  Tools:             │      │  Tools:             │
│  • SQL validation   │      │  • ICD-HCC lookup   │      │  • Pharmacy query   │
│  • Schema checker   │      │  • Coefficient calc │      │  • Claims query     │
│  • Duplicate detect │      │  • Hierarchy rules  │      │  • Clinical RAG     │
└─────────┬───────────┘      └─────────┬───────────┘      └─────────┬───────────┘
          │                            │                            │
          │                            │                            │
          ▼                            ▼                            ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              SHARED TOOL REGISTRY                                        │
│                                                                                          │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐        │
│  │ Databricks │  │ ICD-10     │  │ CMS Ref    │  │ Clinical   │  │ Alert      │        │
│  │ SQL Query  │  │ API        │  │ Tables     │  │ Knowledge  │  │ Service    │        │
│  └────────────┘  └────────────┘  └────────────┘  └────────────┘  └────────────┘        │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Agent Collaboration Protocol

### Message Flow Between Agents

```python
# Agent Communication Protocol

class AgentMessage:
    """Standard message format for inter-agent communication"""
    
    sender: str           # "orchestrator", "risk_scoring_agent", etc.
    receiver: str         # Target agent
    message_type: str     # "task", "result", "query", "error"
    payload: dict         # Task details or results
    correlation_id: str   # Track related messages
    priority: int         # 1=urgent, 5=low
    timestamp: datetime


# Example: Orchestrator assigns task to Risk Scoring Agent
{
    "sender": "orchestrator",
    "receiver": "risk_scoring_agent",
    "message_type": "task",
    "payload": {
        "goal": "Calculate HCC risk scores",
        "member_bids": ["110000001", "110000002", ...],
        "risk_year": 2026,
        "model_version": "V28",
        "dependencies": ["data_quality_complete"]
    },
    "correlation_id": "run_2026_q4_001",
    "priority": 2
}


# Example: Risk Scoring Agent returns results
{
    "sender": "risk_scoring_agent",
    "receiver": "orchestrator",
    "message_type": "result",
    "payload": {
        "status": "success",
        "members_scored": 50000,
        "avg_raf_score": 1.234,
        "errors": [],
        "output_table": "pop_stg.curation.risk_member_output"
    },
    "correlation_id": "run_2026_q4_001"
}
```

---

## 6. Goal Resolution Strategies

### Strategy 1: Divide and Conquer

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│   GOAL: "Process 1M members for risk adjustment"               │
│                                                                 │
│   DECOMPOSITION:                                                │
│                                                                 │
│   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐          │
│   │ Batch 1     │   │ Batch 2     │   │ Batch N     │          │
│   │ 100K members│   │ 100K members│   │ 100K members│          │
│   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘          │
│          │                 │                 │                  │
│          ▼                 ▼                 ▼                  │
│   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐          │
│   │ Agent       │   │ Agent       │   │ Agent       │          │
│   │ Instance 1  │   │ Instance 2  │   │ Instance N  │          │
│   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘          │
│          │                 │                 │                  │
│          └─────────────────┼─────────────────┘                  │
│                            ▼                                    │
│                   ┌─────────────────┐                           │
│                   │   AGGREGATOR    │                           │
│                   │   Merge Results │                           │
│                   └─────────────────┘                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Strategy 2: Expert Consensus

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│   GOAL: "Determine if HCC058 should be suppressed"             │
│                                                                 │
│   EXPERT PANEL:                                                 │
│                                                                 │
│   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐│
│   │ HIERARCHY AGENT │  │ CLINICAL AGENT  │  │ AUDIT AGENT     ││
│   │                 │  │                 │  │                 ││
│   │ "Yes, parent    │  │ "Clinically,    │  │ "Historically,  ││
│   │  HCC057 exists" │  │  these are      │  │  we suppressed  ││
│   │                 │  │  related"       │  │  similar cases" ││
│   │ Vote: SUPPRESS  │  │ Vote: SUPPRESS  │  │ Vote: SUPPRESS  ││
│   └────────┬────────┘  └────────┬────────┘  └────────┬────────┘│
│            │                    │                    │          │
│            └────────────────────┼────────────────────┘          │
│                                 ▼                               │
│                    ┌─────────────────────┐                      │
│                    │   CONSENSUS: 3/3    │                      │
│                    │   → SUPPRESS HCC058 │                      │
│                    └─────────────────────┘                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Strategy 3: Iterative Refinement

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│   GOAL: "Identify all valid gaps for member 123"               │
│                                                                 │
│   ITERATION 1:                                                  │
│   Gap Agent → Found 5 potential gaps                           │
│        │                                                        │
│        ▼                                                        │
│   ITERATION 2:                                                  │
│   Clinical Agent → 2 gaps lack evidence, removed               │
│        │                                                        │
│        ▼                                                        │
│   ITERATION 3:                                                  │
│   Hierarchy Agent → 1 gap suppressed by existing HCC           │
│        │                                                        │
│        ▼                                                        │
│   FINAL: 2 validated gaps with full evidence                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 7. Implementation Example (Python/LangGraph)

```python
from langgraph.graph import StateGraph, END
from typing import TypedDict, Annotated
import operator

# Define shared state
class RiskAdjustmentState(TypedDict):
    goal: str
    member_bids: list[str]
    risk_year: int
    
    # Results from each agent
    data_quality_result: dict
    risk_scores: dict
    gaps_identified: list
    
    # Tracking
    errors: Annotated[list, operator.add]
    status: str


# Define agent nodes
def orchestrator_node(state: RiskAdjustmentState) -> dict:
    """Orchestrator decides next step based on current state"""
    
    if state["status"] == "init":
        return {"status": "validate_data"}
    elif state["status"] == "data_validated":
        return {"status": "calculate_scores"}
    elif state["status"] == "scores_calculated":
        return {"status": "identify_gaps"}
    elif state["status"] == "gaps_identified":
        return {"status": "complete"}
    
    return {"status": "error"}


def data_quality_agent(state: RiskAdjustmentState) -> dict:
    """Validates data quality before processing"""
    
    # Agent reasoning: Check claims, find issues, fix what's fixable
    result = {
        "valid_members": 49500,
        "invalid_members": 500,
        "issues_fixed": 450,
        "issues_flagged": 50
    }
    
    return {
        "data_quality_result": result,
        "status": "data_validated"
    }


def risk_scoring_agent(state: RiskAdjustmentState) -> dict:
    """Calculates HCC risk scores with clinical reasoning"""
    
    # Agent uses tools: ICD lookup, coefficient calc, hierarchy check
    scores = {
        "members_scored": 49500,
        "avg_raf": 1.234,
        "high_risk_count": 5000
    }
    
    return {
        "risk_scores": scores,
        "status": "scores_calculated"
    }


def gap_analysis_agent(state: RiskAdjustmentState) -> dict:
    """Identifies potential HCC gaps with evidence"""
    
    # Agent reasons about pharmacy + claims + clinical context
    gaps = [
        {"member": "123", "hcc": "HCC019", "confidence": 0.87, "evidence": "..."},
        {"member": "456", "hcc": "HCC085", "confidence": 0.92, "evidence": "..."},
    ]
    
    return {
        "gaps_identified": gaps,
        "status": "gaps_identified"
    }


# Build the graph
def build_multi_agent_graph():
    
    workflow = StateGraph(RiskAdjustmentState)
    
    # Add nodes
    workflow.add_node("orchestrator", orchestrator_node)
    workflow.add_node("data_quality", data_quality_agent)
    workflow.add_node("risk_scoring", risk_scoring_agent)
    workflow.add_node("gap_analysis", gap_analysis_agent)
    
    # Define routing logic
    def route_next(state):
        status = state["status"]
        if status == "validate_data":
            return "data_quality"
        elif status == "calculate_scores":
            return "risk_scoring"
        elif status == "identify_gaps":
            return "gap_analysis"
        elif status == "complete":
            return END
        return "orchestrator"
    
    # Add edges
    workflow.set_entry_point("orchestrator")
    workflow.add_conditional_edges("orchestrator", route_next)
    workflow.add_edge("data_quality", "orchestrator")
    workflow.add_edge("risk_scoring", "orchestrator")
    workflow.add_edge("gap_analysis", "orchestrator")
    
    return workflow.compile()


# Execute
app = build_multi_agent_graph()
result = app.invoke({
    "goal": "Process Q4 2026 risk adjustment",
    "member_bids": ["123", "456", ...],
    "risk_year": 2026,
    "status": "init"
})
```

---

## 8. Interview Answer Framework

### **1. Define the Concept (30 seconds)**
> "Multi-Agent Decision Frameworks involve multiple specialized AI agents working together to solve complex problems. Each agent has specific expertise, and an orchestrator coordinates their collaboration toward a shared goal."

### **2. Patterns I Use (1 minute)**
> "I architect these systems using four main patterns:
> - **Hierarchical** - A supervisor agent delegates to specialists (best for clear task decomposition)
> - **Peer-to-Peer** - Agents collaborate directly (best for consensus decisions)
> - **Pipeline** - Sequential handoff between agents (best for ETL-style workflows)
> - **Debate** - Proposer and critic agents with a judge (best for high-stakes decisions)"

### **3. Concrete Example (1.5 minutes)**
> "In my risk adjustment platform, I designed a multi-agent framework where:
> 
> - **Orchestrator Agent** receives the goal 'Process Q4 risk adjustment for 1M members'
> - It delegates to **Data Quality Agent** (validates claims, fixes anomalies)
> - Then **Risk Scoring Agent** (maps ICD→HCC, calculates RAF scores)
> - Then **Gap Analysis Agent** (identifies undocumented conditions)
> - Finally **Clinical Reasoning Agent** provides evidence-based validation
> 
> Agents communicate via a standard message protocol and share tools like SQL queries and clinical knowledge bases. The orchestrator handles failures, retries, and aggregates final results."

### **4. Goal Resolution (30 seconds)**
> "For goal resolution, I use strategies like:
> - **Divide and Conquer** for parallelizable work (batch processing)
> - **Expert Consensus** for ambiguous decisions (voting among specialists)
> - **Iterative Refinement** for quality-critical outputs (multiple passes)"

---

## 9. Key Talking Points

| Topic | Your Answer |
|-------|-------------|
| **Why Multi-Agent?** | Single agents struggle with complex, multi-step problems; specialists collaborate better |
| **Orchestration** | Hierarchical for control, peer-to-peer for reasoning, pipeline for ETL |
| **Goal Resolution** | Decomposition, consensus, iteration - choose based on problem type |
| **Healthcare Specifics** | Clinical reasoning, audit trails, human-in-the-loop for high-stakes |
| **Frameworks** | LangGraph, AutoGen, Semantic Kernel, CrewAI |
| **Failure Handling** | Circuit breakers, retries, fallback to human review |

---

# Population Advyzer - Complete Architecture Reference

## High-Level System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              POPULATION ADVYZER PLATFORM                                 │
│                    Medicare Advantage / CMS Risk Adjustment System                       │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌──────────────┐ │
│  │   DATA SOURCES  │    │  DATA PLATFORM  │    │  PROCESSING     │    │   OUTPUTS    │ │
│  │                 │    │                 │    │                 │    │              │ │
│  │ • Raw Files     │───▶│ • Databricks    │───▶│ • Risk Scoring  │───▶│ • Dashboards │ │
│  │ • Delta Share   │    │ • Unity Catalog │    │ • Gap Analysis  │    │ • Reports    │ │
│  │ • CMS Downloads │    │ • Delta Lake    │    │ • Persistence   │    │ • Snowflake  │ │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘    └──────────────┘ │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## Data Pipeline Architecture (Medallion)

```
BRONZE (Ingestion)          SILVER (Transformation)         GOLD (Curation)
─────────────────           ───────────────────────         ───────────────

Raw Files ───▶ data_loader_main.py ───▶ ingestion_to_transformation.py ───▶ ma_model_input_data.py
                     │                            │                                │
                     ▼                            ▼                                ▼
            {plan}ingestion              {plan}transformation              {plan}curation
            schema                       schema                            schema
```

## CMS Risk Scoring Engine (13 Steps)

```
Step 1-3:  Demographics (Original Disability → Community Model → Age/Sex SEDITs)
Step 4-6:  HCC Mapping (ICD → HCC → Hierarchy → Suppress Child)
Step 7-9:  Scoring (Assign Coefficients → Interactions → Final Raw Score)
Step 10-13: Finalization (Normalize → Weightage → Metadata → Output)
```

## Gap Suspecting Methods

| Method | Input | Logic |
|--------|-------|-------|
| Method 1 | Pharmacy (NDC) | NDC code match against metadata |
| Method 2 | Medical Claims | Diagnosis/CPT/ICD-PROC match with qualification |
| Method 10 | Procedures | CPT/ICD + 180-day exclusion window |
| Method 4 | Persistent CCs | Prior year condition persistence |

## Suppression Logic

1. **SUPPRESSION1**: Single-method parent-child hierarchy
2. **SUPPRESSION2**: Existing member CCs (parent already captured)
3. **CROSS-METHOD**: Across all methods for same member

---

---

# Question 3: Engineer fault-tolerant agent systems with end-to-end observability, monitoring, and self-healing capabilities

---

## 1. What Does This Mean?

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    FAULT-TOLERANT AGENT SYSTEMS                                          │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   ┌─────────────────────┐   ┌─────────────────────┐   ┌─────────────────────┐          │
│   │   FAULT TOLERANCE   │   │   OBSERVABILITY     │   │   SELF-HEALING      │          │
│   │                     │   │                     │   │                     │          │
│   │ System continues    │   │ Full visibility     │   │ Auto-recovery from  │          │
│   │ working despite     │   │ into system state   │   │ failures without    │          │
│   │ component failures  │   │ and behavior        │   │ human intervention  │          │
│   │                     │   │                     │   │                     │          │
│   │ • Retries           │   │ • Logs              │   │ • Health checks     │          │
│   │ • Circuit breakers  │   │ • Metrics           │   │ • Auto-restart      │          │
│   │ • Fallbacks         │   │ • Traces            │   │ • Auto-scale        │          │
│   │ • Graceful degrade  │   │ • Dashboards        │   │ • Rollback          │          │
│   └─────────────────────┘   └─────────────────────┘   └─────────────────────┘          │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Why Critical for AI Agents?

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                     AI AGENTS HAVE UNIQUE FAILURE MODES                                  │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   Traditional Software              AI Agent Systems                                     │
│   ────────────────────              ────────────────────                                 │
│                                                                                          │
│   • Deterministic failures          • Non-deterministic outputs                         │
│   • Clear error codes               • LLM API rate limits / timeouts                    │
│   • Predictable behavior            • Token limit exceeded                              │
│   • Known failure modes             • Hallucinations / wrong tool calls                 │
│                                     • Context window overflow                           │
│                                     • Model degradation over time                       │
│                                     • Prompt injection attacks                          │
│                                     • Cost runaway (infinite loops)                     │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Fault Tolerance Patterns for AI Agents

### Pattern 1: Retry with Exponential Backoff

```python
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class AgentRetryPolicy:
    """
    Retry policy for LLM API calls with exponential backoff.
    """
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((RateLimitError, TimeoutError, APIError))
    )
    async def call_llm(self, prompt: str) -> str:
        """Call LLM with automatic retry on transient failures"""
        response = await self.llm_client.complete(prompt)
        return response
```

```
┌─────────────────────────────────────────────────────────────────┐
│                    RETRY WITH BACKOFF                           │
│                                                                 │
│   Request 1 ──▶ FAIL (429 Rate Limit)                          │
│       │                                                         │
│       ▼ wait 2s                                                 │
│   Request 2 ──▶ FAIL (Timeout)                                 │
│       │                                                         │
│       ▼ wait 4s                                                 │
│   Request 3 ──▶ SUCCESS ✓                                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Pattern 2: Circuit Breaker

```python
from circuitbreaker import circuit

class LLMCircuitBreaker:
    """
    Prevents cascading failures when LLM service is degraded.
    """
    
    @circuit(
        failure_threshold=5,      # Open after 5 failures
        recovery_timeout=60,      # Try again after 60s
        expected_exception=LLMServiceError
    )
    async def call_agent(self, task: str) -> str:
        """
        Circuit states:
        - CLOSED: Normal operation, requests pass through
        - OPEN: Service down, fail fast without calling
        - HALF-OPEN: Testing if service recovered
        """
        return await self.agent.run(task)
```

```
┌─────────────────────────────────────────────────────────────────┐
│                    CIRCUIT BREAKER STATES                       │
│                                                                 │
│   ┌──────────┐     5 failures    ┌──────────┐                  │
│   │  CLOSED  │ ─────────────────▶│   OPEN   │                  │
│   │ (Normal) │                   │(Fail Fast)│                  │
│   └────▲─────┘                   └─────┬─────┘                  │
│        │                               │                        │
│        │ success                       │ 60s timeout            │
│        │                               ▼                        │
│        │                        ┌───────────┐                   │
│        └────────────────────────│ HALF-OPEN │                   │
│                    success      │  (Test)   │                   │
│                                 └───────────┘                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Pattern 3: Fallback Chain

```python
class AgentFallbackChain:
    """
    Try multiple strategies when primary fails.
    """
    
    async def analyze_gap(self, member_data: dict) -> GapResult:
        """Fallback chain for gap analysis"""
        
        # Try 1: Full AI reasoning
        try:
            return await self.clinical_reasoning_agent.analyze(member_data)
        except AgentError:
            logger.warning("AI agent failed, falling back to rule-based")
        
        # Try 2: Rule-based system (current Pop Advyzer logic)
        try:
            return self.rule_based_gap_detector.analyze(member_data)
        except Exception:
            logger.warning("Rule-based failed, falling back to simple match")
        
        # Try 3: Simple pattern matching
        return self.simple_ndc_matcher.analyze(member_data)
```

```
┌─────────────────────────────────────────────────────────────────┐
│                    FALLBACK CHAIN                               │
│                                                                 │
│   ┌─────────────────┐                                          │
│   │ AI REASONING    │ ──▶ Success? ──▶ Return Result           │
│   │ (Best Quality)  │         │                                │
│   └─────────────────┘         │ Fail                           │
│                               ▼                                 │
│   ┌─────────────────┐                                          │
│   │ RULE-BASED      │ ──▶ Success? ──▶ Return Result           │
│   │ (Good Quality)  │         │                                │
│   └─────────────────┘         │ Fail                           │
│                               ▼                                 │
│   ┌─────────────────┐                                          │
│   │ SIMPLE MATCH    │ ──▶ Return Result (Best Effort)          │
│   │ (Basic Quality) │                                          │
│   └─────────────────┘                                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Pattern 4: Timeout and Cancellation

```python
import asyncio

class AgentTimeoutPolicy:
    """
    Prevent runaway agent executions.
    """
    
    async def run_with_timeout(self, agent, task: str, timeout_seconds: int = 300):
        """Run agent with timeout protection"""
        
        try:
            result = await asyncio.wait_for(
                agent.run(task),
                timeout=timeout_seconds
            )
            return result
            
        except asyncio.TimeoutError:
            logger.error(f"Agent timed out after {timeout_seconds}s")
            await agent.cancel()  # Clean up resources
            raise AgentTimeoutError(f"Task exceeded {timeout_seconds}s limit")
```

### Pattern 5: Cost Circuit Breaker

```python
class CostGuard:
    """
    Prevent cost runaway from infinite loops or excessive API calls.
    """
    
    def __init__(self, max_tokens_per_run: int = 100000, max_cost_usd: float = 10.0):
        self.max_tokens = max_tokens_per_run
        self.max_cost = max_cost_usd
        self.tokens_used = 0
        self.cost_usd = 0.0
    
    def check_budget(self, tokens_consumed: int, cost: float):
        self.tokens_used += tokens_consumed
        self.cost_usd += cost
        
        if self.tokens_used > self.max_tokens:
            raise CostLimitExceeded(f"Token limit exceeded: {self.tokens_used}")
        
        if self.cost_usd > self.max_cost:
            raise CostLimitExceeded(f"Cost limit exceeded: ${self.cost_usd:.2f}")
```

---

## 4. End-to-End Observability (Three Pillars)

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         THREE PILLARS OF OBSERVABILITY                                   │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   ┌─────────────────────┐   ┌─────────────────────┐   ┌─────────────────────┐          │
│   │       LOGS          │   │      METRICS        │   │      TRACES         │          │
│   │                     │   │                     │   │                     │          │
│   │ What happened?      │   │ How is it doing?    │   │ Where did it go?    │          │
│   │                     │   │                     │   │                     │          │
│   │ • Structured events │   │ • Counters          │   │ • Request path      │          │
│   │ • Error details     │   │ • Gauges            │   │ • Agent handoffs    │          │
│   │ • Agent reasoning   │   │ • Histograms        │   │ • Tool calls        │          │
│   │ • Tool call results │   │ • SLIs              │   │ • Latency breakdown │          │
│   └─────────────────────┘   └─────────────────────┘   └─────────────────────┘          │
│                                                                                          │
│   Tools: Azure Monitor     Tools: Prometheus       Tools: OpenTelemetry                 │
│          Application       Azure Monitor           Jaeger, Zipkin                       │
│          Insights          Grafana                 Azure App Insights                   │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Structured Logging for Agents

```python
import structlog
from opentelemetry import trace

logger = structlog.get_logger()
tracer = trace.get_tracer(__name__)

class ObservableAgent:
    """
    Agent with comprehensive observability.
    """
    
    async def run(self, task: str) -> str:
        # Start distributed trace
        with tracer.start_as_current_span("agent.run") as span:
            span.set_attribute("agent.name", self.name)
            span.set_attribute("task.id", task.id)
            
            # Structured log entry
            logger.info(
                "agent_task_started",
                agent=self.name,
                task_id=task.id,
                goal=task.goal,
                member_count=len(task.member_bids)
            )
            
            try:
                result = await self._execute(task)
                
                # Success metrics
                logger.info(
                    "agent_task_completed",
                    agent=self.name,
                    task_id=task.id,
                    duration_ms=span.duration_ms,
                    tokens_used=result.tokens,
                    tool_calls=result.tool_call_count
                )
                
                return result
                
            except Exception as e:
                # Error with full context
                logger.error(
                    "agent_task_failed",
                    agent=self.name,
                    task_id=task.id,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    stack_trace=traceback.format_exc()
                )
                span.record_exception(e)
                raise
```

### Key Metrics for AI Agents

```python
from prometheus_client import Counter, Histogram, Gauge

# Agent Metrics
agent_requests_total = Counter(
    'agent_requests_total',
    'Total agent requests',
    ['agent_name', 'status']  # status: success, failure, timeout
)

agent_latency_seconds = Histogram(
    'agent_latency_seconds',
    'Agent response time',
    ['agent_name'],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60, 120]
)

agent_tokens_used = Counter(
    'agent_tokens_used_total',
    'Total tokens consumed',
    ['agent_name', 'model']
)

agent_tool_calls = Counter(
    'agent_tool_calls_total',
    'Tool calls made by agents',
    ['agent_name', 'tool_name', 'status']
)

agent_cost_usd = Counter(
    'agent_cost_usd_total',
    'Total cost in USD',
    ['agent_name', 'model']
)

# Circuit breaker state
circuit_breaker_state = Gauge(
    'circuit_breaker_state',
    'Circuit breaker state (0=closed, 1=open, 0.5=half-open)',
    ['service_name']
)

# Active agents
active_agents = Gauge(
    'active_agents',
    'Currently running agent instances',
    ['agent_name']
)
```

### Distributed Tracing for Multi-Agent

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    DISTRIBUTED TRACE: Risk Adjustment Pipeline                           │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  TraceID: abc123                                                                         │
│                                                                                          │
│  ├─ orchestrator.run [2.5s]                                                             │
│  │  ├─ parse_goal [50ms]                                                                │
│  │  ├─ delegate_to_data_quality [800ms]                                                 │
│  │  │  ├─ data_quality_agent.run [750ms]                                                │
│  │  │  │  ├─ tool.sql_query [200ms] → 50K claims validated                             │
│  │  │  │  ├─ llm.complete [400ms] → 15K tokens                                         │
│  │  │  │  └─ tool.fix_anomalies [150ms] → 450 fixed                                    │
│  │  │                                                                                    │
│  │  ├─ delegate_to_risk_scoring [1.2s]                                                  │
│  │  │  ├─ risk_scoring_agent.run [1.1s]                                                 │
│  │  │  │  ├─ tool.icd_hcc_lookup [100ms]                                               │
│  │  │  │  ├─ llm.complete [600ms] → 20K tokens                                         │
│  │  │  │  ├─ tool.coefficient_calc [200ms]                                             │
│  │  │  │  └─ tool.write_delta [200ms]                                                  │
│  │  │                                                                                    │
│  │  └─ delegate_to_gap_analysis [450ms]                                                 │
│  │     ├─ gap_analysis_agent.run [400ms]                                                │
│  │     │  ├─ tool.pharmacy_query [100ms]                                               │
│  │     │  ├─ llm.complete [200ms] → 8K tokens                                          │
│  │     │  └─ tool.clinical_rag [100ms]                                                 │
│  │                                                                                       │
│  └─ aggregate_results [50ms]                                                            │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Monitoring & Alerting

### Dashboard Design

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                       AGENT MONITORING DASHBOARD                                         │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  ┌─────────────────────────────────┐  ┌─────────────────────────────────┐              │
│  │     AGENT HEALTH STATUS         │  │      REQUEST RATE               │              │
│  │                                 │  │                                 │              │
│  │  Orchestrator    ● HEALTHY      │  │  ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄   │              │
│  │  DataQuality     ● HEALTHY      │  │  ████████████████████████████   │              │
│  │  RiskScoring     ● DEGRADED     │  │                                 │              │
│  │  GapAnalysis     ● HEALTHY      │  │  500 req/min avg                │              │
│  └─────────────────────────────────┘  └─────────────────────────────────┘              │
│                                                                                          │
│  ┌─────────────────────────────────┐  ┌─────────────────────────────────┐              │
│  │     LATENCY (P95)               │  │      ERROR RATE                 │              │
│  │                                 │  │                                 │              │
│  │  ▁▂▃▄▅▆▇█▇▆▅▄▃▂▁▂▃▄▅▆          │  │  ▁▁▁▁▂▁▁▁▁█▁▁▁▁▁▁▁▁▁▁▁         │              │
│  │                                 │  │         ↑                       │              │
│  │  2.5s current (SLO: 5s) ✓       │  │      Spike at 14:30             │              │
│  └─────────────────────────────────┘  └─────────────────────────────────┘              │
│                                                                                          │
│  ┌─────────────────────────────────┐  ┌─────────────────────────────────┐              │
│  │     TOKEN USAGE / COST          │  │      CIRCUIT BREAKER STATUS     │              │
│  │                                 │  │                                 │              │
│  │  Today: 2.5M tokens ($45)       │  │  LLM Service:    CLOSED ●       │              │
│  │  Budget: 5M tokens ($100)       │  │  Databricks:     CLOSED ●       │              │
│  │  ████████████░░░░░░░ 50%        │  │  Clinical RAG:   HALF-OPEN ◐    │              │
│  └─────────────────────────────────┘  └─────────────────────────────────┘              │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Alert Rules

```yaml
# alerts.yaml - PagerDuty/OpsGenie Alert Rules

alerts:
  # Critical - Page immediately
  - name: agent_complete_failure
    condition: agent_requests_total{status="failure"} / agent_requests_total > 0.5
    duration: 5m
    severity: critical
    message: "Agent failure rate >50% for 5 minutes"
    
  - name: circuit_breaker_open
    condition: circuit_breaker_state{service_name="llm_service"} == 1
    duration: 1m
    severity: critical
    message: "LLM service circuit breaker is OPEN"
    
  - name: cost_runaway
    condition: rate(agent_cost_usd_total[1h]) > 50
    severity: critical
    message: "Agent cost exceeding $50/hour"
  
  # Warning - Notify team
  - name: high_latency
    condition: histogram_quantile(0.95, agent_latency_seconds) > 30
    duration: 10m
    severity: warning
    message: "Agent P95 latency >30s"
    
  - name: token_budget_80_percent
    condition: agent_tokens_used_total > budget_tokens * 0.8
    severity: warning
    message: "Token usage at 80% of daily budget"
    
  # Info - Log only
  - name: retry_rate_elevated
    condition: rate(agent_retries_total[5m]) > 10
    severity: info
    message: "Elevated retry rate detected"
```

---

## 6. Self-Healing Capabilities

### Health Check System

```python
from enum import Enum
from dataclasses import dataclass

class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"

@dataclass
class HealthCheckResult:
    status: HealthStatus
    checks: dict
    message: str

class AgentHealthChecker:
    """
    Comprehensive health checks for agent systems.
    """
    
    async def check_health(self) -> HealthCheckResult:
        checks = {}
        
        # Check 1: LLM API connectivity
        checks["llm_api"] = await self._check_llm_api()
        
        # Check 2: Database connectivity
        checks["databricks"] = await self._check_databricks()
        
        # Check 3: Tool availability
        checks["tools"] = await self._check_tools()
        
        # Check 4: Memory/resource usage
        checks["resources"] = self._check_resources()
        
        # Check 5: Recent success rate
        checks["success_rate"] = await self._check_success_rate()
        
        # Determine overall status
        if all(c["status"] == "healthy" for c in checks.values()):
            return HealthCheckResult(HealthStatus.HEALTHY, checks, "All systems operational")
        elif any(c["status"] == "unhealthy" for c in checks.values()):
            return HealthCheckResult(HealthStatus.UNHEALTHY, checks, "Critical component failure")
        else:
            return HealthCheckResult(HealthStatus.DEGRADED, checks, "Some components degraded")
    
    async def _check_llm_api(self) -> dict:
        try:
            start = time.time()
            await self.llm_client.complete("health check", max_tokens=5)
            latency = time.time() - start
            
            if latency < 2:
                return {"status": "healthy", "latency_ms": latency * 1000}
            else:
                return {"status": "degraded", "latency_ms": latency * 1000}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}
```

### Auto-Recovery Procedures

```python
class SelfHealingAgent:
    """
    Agent with automatic recovery capabilities.
    """
    
    async def run_with_self_healing(self, task: str) -> str:
        """Execute task with automatic recovery on failure"""
        
        for attempt in range(self.max_recovery_attempts):
            try:
                return await self._run(task)
                
            except MemoryError:
                logger.warning("Memory pressure detected, clearing cache")
                await self._clear_cache()
                await self._garbage_collect()
                
            except ConnectionError:
                logger.warning("Connection lost, reconnecting")
                await self._reconnect_services()
                
            except RateLimitError:
                logger.warning("Rate limited, switching to backup model")
                self._switch_to_backup_model()
                
            except ContextLengthExceeded:
                logger.warning("Context too long, summarizing history")
                await self._summarize_and_truncate_context()
                
            except AgentStuck:
                logger.warning("Agent stuck, resetting state")
                await self._reset_agent_state()
        
        # All recovery attempts failed
        raise UnrecoverableError(f"Failed after {self.max_recovery_attempts} attempts")
    
    async def _clear_cache(self):
        """Clear agent memory cache to free resources"""
        self.memory.clear()
        gc.collect()
    
    async def _reconnect_services(self):
        """Re-establish connections to external services"""
        await self.llm_client.reconnect()
        await self.db_client.reconnect()
        await self.tool_registry.refresh()
    
    def _switch_to_backup_model(self):
        """Switch from GPT-4 to GPT-3.5 or local model"""
        self.model = self.backup_model
        logger.info(f"Switched to backup model: {self.model}")
    
    async def _summarize_and_truncate_context(self):
        """Compress conversation history to fit context window"""
        summary = await self.summarizer.summarize(self.conversation_history)
        self.conversation_history = [{"role": "system", "content": summary}]
```

### Kubernetes Self-Healing Configuration

```yaml
# k8s-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: risk-scoring-agent
spec:
  replicas: 3
  selector:
    matchLabels:
      app: risk-scoring-agent
  template:
    spec:
      containers:
      - name: agent
        image: agents/risk-scoring:latest
        
        # Health probes for auto-restart
        livenessProbe:
          httpGet:
            path: /health/live
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
          failureThreshold: 3  # Restart after 3 failures
          
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8080
          periodSeconds: 5
          failureThreshold: 2  # Remove from LB after 2 failures
        
        # Resource limits to prevent runaway
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
            
---
# Horizontal Pod Autoscaler
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: risk-scoring-agent-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: risk-scoring-agent
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Pods
    pods:
      metric:
        name: agent_queue_depth
      target:
        type: AverageValue
        averageValue: 100
```

---

## 7. Complete Architecture for Population Advyzer

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│              FAULT-TOLERANT AGENT SYSTEM ARCHITECTURE                                    │
└──────────────────────────────────────────────────────────────────────────────────────────┘

                                    ┌─────────────────┐
                                    │   API GATEWAY   │
                                    │   (Rate Limit)  │
                                    └────────┬────────┘
                                             │
                                    ┌────────▼────────┐
                                    │  LOAD BALANCER  │
                                    └────────┬────────┘
                                             │
         ┌───────────────────────────────────┼───────────────────────────────────┐
         │                                   │                                   │
         ▼                                   ▼                                   ▼
┌─────────────────┐              ┌─────────────────┐              ┌─────────────────┐
│  ORCHESTRATOR   │              │  ORCHESTRATOR   │              │  ORCHESTRATOR   │
│   INSTANCE 1    │              │   INSTANCE 2    │              │   INSTANCE 3    │
│                 │              │                 │              │                 │
│ ┌─────────────┐ │              │ ┌─────────────┐ │              │ ┌─────────────┐ │
│ │Circuit Break│ │              │ │Circuit Break│ │              │ │Circuit Break│ │
│ │   + Retry   │ │              │ │   + Retry   │ │              │ │   + Retry   │ │
│ └─────────────┘ │              │ └─────────────┘ │              │ └─────────────┘ │
└────────┬────────┘              └────────┬────────┘              └────────┬────────┘
         │                                │                                │
         └────────────────────────────────┼────────────────────────────────┘
                                          │
                            ┌─────────────┴─────────────┐
                            │                           │
                            ▼                           ▼
                 ┌─────────────────────┐    ┌─────────────────────┐
                 │    PRIMARY LLM      │    │    BACKUP LLM       │
                 │   (Azure OpenAI)    │    │   (Azure OpenAI     │
                 │                     │    │    Secondary)       │
                 │  GPT-4o             │    │  GPT-3.5-turbo      │
                 └──────────┬──────────┘    └──────────┬──────────┘
                            │                          │
                            │     ┌────────────────────┘
                            │     │ Fallback
                            ▼     ▼
                 ┌─────────────────────────────────────────────────┐
                 │              TOOL LAYER                         │
                 │                                                 │
                 │  ┌──────────┐ ┌──────────┐ ┌──────────┐       │
                 │  │Databricks│ │Azure AI  │ │ Custom   │       │
                 │  │SQL       │ │Search    │ │ APIs     │       │
                 │  │(Primary) │ │(RAG)     │ │          │       │
                 │  └────┬─────┘ └──────────┘ └──────────┘       │
                 │       │                                        │
                 │       ▼                                        │
                 │  ┌──────────┐                                  │
                 │  │Databricks│ Connection Pool + Retry          │
                 │  │(Replica) │                                  │
                 │  └──────────┘                                  │
                 └─────────────────────────────────────────────────┘
                                          │
                            ┌─────────────┴─────────────┐
                            │                           │
                            ▼                           ▼
                 ┌─────────────────────┐    ┌─────────────────────┐
                 │   OBSERVABILITY     │    │    SELF-HEALING     │
                 │                     │    │                     │
                 │ • Azure Monitor     │    │ • Health Checks     │
                 │ • App Insights      │    │ • Auto-restart      │
                 │ • Log Analytics     │    │ • Auto-scale        │
                 │ • Distributed Trace │    │ • Circuit Breakers  │
                 │ • Custom Dashboards │    │ • Fallback Chains   │
                 └─────────────────────┘    └─────────────────────┘
```

---

## 8. Interview Answer Framework

### **1. Context (30 seconds)**
> "AI agent systems have unique failure modes - LLM rate limits, hallucinations, token overflows, cost runaway. I engineer systems that handle these gracefully while providing full visibility into agent behavior."

### **2. Fault Tolerance (1 minute)**
> "I implement multiple resilience patterns:
> - **Retry with exponential backoff** for transient LLM API failures
> - **Circuit breakers** to fail fast when services are down
> - **Fallback chains** where AI agents fall back to rule-based systems
> - **Cost guards** to prevent runaway token consumption
> - **Timeout policies** to kill stuck agent loops"

### **3. Observability (1 minute)**
> "For end-to-end observability, I implement the three pillars:
> - **Structured logs** capturing agent reasoning, tool calls, and decisions
> - **Metrics** for latency, error rates, token usage, and cost
> - **Distributed traces** showing the full path through multi-agent workflows
> 
> In Azure, I use Application Insights with custom dashboards showing agent health, circuit breaker states, and budget consumption."

### **4. Self-Healing (30 seconds)**
> "Self-healing includes:
> - Kubernetes liveness/readiness probes for auto-restart
> - Horizontal pod autoscaling based on queue depth
> - Automatic model fallback when primary LLM is degraded
> - Context summarization when token limits are exceeded
> - Automatic cache clearing under memory pressure"

---

## 9. Key Talking Points

| Topic | Your Answer |
|-------|-------------|
| **Why different from traditional?** | AI agents have non-deterministic failures, cost concerns, and can get stuck in loops |
| **Circuit breaker purpose** | Fail fast to prevent cascading failures; give services time to recover |
| **Fallback strategy** | AI → Rule-based → Simple match; graceful degradation preserves functionality |
| **Key metrics for agents** | Latency, error rate, token usage, cost, tool call success rate |
| **Self-healing examples** | Auto-restart, model fallback, context compression, cache clearing |
| **Healthcare specifics** | Audit logs for compliance, human-in-the-loop for clinical decisions |

---

---

# Population Advyzer - Complete End-to-End Architecture

## Ultimate Business Goal

The complete data flow delivers **actionable insights to business users via Tableau dashboards**, with data flowing from raw sources through Databricks processing to Snowflake and finally to visualization.

### Key Output Tables for Tableau

| Table | Purpose |
|-------|---------|
| `pop_stg.uatplan1_ma_dashboard.member_level` | Member-level demographics, risk scores, enrollment |
| `pop_stg.uatplan1_ma_dashboard.member_hcc_level` | Member HCC details, gaps, confidence factors |

---

## Complete Data Flow (End-to-End)

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         COMPLETE DATA FLOW (End-to-End)                                  │
└──────────────────────────────────────────────────────────────────────────────────────────┘

  DATA SOURCES              DATABRICKS                 SNOWFLAKE              TABLEAU
  ────────────              ──────────                 ─────────              ───────

  ┌─────────────┐
  │ Raw Files   │───┐
  │ • Member    │   │
  │ • Claims    │   │
  │ • Pharmacy  │   │
  └─────────────┘   │
                    │      ┌────────────────────────────────────────────┐
  ┌─────────────┐   │      │           DATABRICKS (Unity Catalog)       │
  │ Delta Share │───┼─────▶│                                            │
  │ • MMR       │   │      │  BRONZE         SILVER          GOLD       │
  │ • MAO-004   │   │      │  (Ingestion) → (Transform) → (Curation)   │
  │ • MOR       │   │      │                                            │
  └─────────────┘   │      │         ┌──────────────────────┐           │
                    │      │         │   PROCESSING         │           │
  ┌─────────────┐   │      │         │   • Risk Scoring     │           │
  │ CMS Scrape  │───┘      │         │   • Gap Suspecting   │           │
  │ • PBP Files │          │         │   • Persistence      │           │
  └─────────────┘          │         │   • Interactions     │           │
                           │         └──────────┬───────────┘           │
                           │                    │                       │
                           │                    ▼                       │
                           │  ┌─────────────────────────────────────┐  │
                           │  │      MA DASHBOARD SCHEMA            │  │
                           │  │                                     │  │
                           │  │  • member_level                     │  │
                           │  │  • member_hcc_level                 │  │
                           │  │  • pcp_attribution                  │  │
                           │  │  • health_system_normalized         │  │
                           │  │  • mmr_flat (MLR metrics)           │  │
                           │  │                                     │  │
                           │  │  Format: Delta + Iceberg Universal  │  │
                           │  └─────────────────┬───────────────────┘  │
                           │                    │                       │
                           └────────────────────┼───────────────────────┘
                                                │
                                                │ Iceberg Catalog
                                                │ (Delta Universal Format)
                                                ▼
                           ┌────────────────────────────────────────────┐
                           │              SNOWFLAKE                     │
                           │                                            │
                           │  External Tables via Iceberg Catalog       │
                           │  • Read-only access to Delta tables        │
                           │  • No data duplication                     │
                           │  • Real-time data availability             │
                           │                                            │
                           └────────────────────┬───────────────────────┘
                                                │
                                                │ Snowflake Connector
                                                ▼
                           ┌────────────────────────────────────────────┐
                           │              TABLEAU                       │
                           │                                            │
                           │  DASHBOARDS:                               │
                           │  • Member Risk Score Analysis              │
                           │  • HCC Gap Closure Tracking                │
                           │  • Plan Performance Metrics                │
                           │  • Provider Attribution                    │
                           │  • MLR / Financial Analytics               │
                           │                                            │
                           │  USERS:                                    │
                           │  • Health Plan Analysts                    │
                           │  • Clinical Operations                     │
                           │  • Finance / Actuarial                     │
                           │  • Executive Leadership                    │
                           │                                            │
                           └────────────────────────────────────────────┘
```

---

## Why This Architecture Matters for AI Architect Role

### 1. Enterprise-Scale Deployment
- Not just processing data, but delivering **business value** to end users
- Multi-platform integration (Databricks → Snowflake → Tableau)
- Supports hundreds of concurrent dashboard users

### 2. Data Interoperability
- **Iceberg Universal Format** enables cross-platform access
- No ETL required between Databricks and Snowflake
- Single source of truth in Delta Lake

### 3. Agentic AI Integration Points

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    WHERE AGENTS ADD VALUE IN THE PIPELINE                                │
└──────────────────────────────────────────────────────────────────────────────────────────┘

  CURRENT (Rule-Based)                      FUTURE (Agentic AI)
  ────────────────────                      ───────────────────

  Data Ingestion                            Data Quality Agent
  • Schema validation                       • Anomaly detection with reasoning
  • Fixed rules                             • Auto-correction with explanation

  Risk Scoring                              Risk Scoring Agent
  • ICD→HCC lookup tables                   • Clinical context interpretation
  • Static coefficients                     • Edge case handling

  Gap Suspecting                            Gap Analysis Agent
  • NDC/CPT pattern match                   • Evidence-based gap identification
  • Fixed suppression rules                 • Confidence scoring with reasoning

  ────────────────────────────────────────────────────────────────────────────────
                                    │
                                    ▼
  MA Dashboard Tables               Dashboard Intelligence Agent
  • Static aggregations             • Natural language queries
  • Pre-defined metrics             • Anomaly alerts with explanation
                                    • Predictive insights

  Tableau Dashboards                Conversational BI Agent
  • Manual exploration              • "Why did RAF drop for Plan X?"
  • Static reports                  • "Which members have new gaps?"
                                    • Auto-generated insights
```

### 4. Interview Talking Point

> "The system I architected doesn't just process data—it delivers actionable insights to business users. Data flows from raw claims through Databricks processing, gets exposed to Snowflake via Iceberg format, and powers Tableau dashboards that health plan analysts use daily. 
>
> With Agentic AI, we can add a **Conversational BI Agent** that lets users ask natural language questions like 'Why did our average RAF score drop this quarter?' and get evidence-based answers by querying the underlying `member_level` and `member_hcc_level` tables."

---

---

---

# Question 4: Design workflow management systems governing agent and node interactions, sequencing, and state management

---

## 1. What is Agent Workflow Management?

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                        AGENT WORKFLOW MANAGEMENT                                         │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   Managing HOW agents execute tasks:                                                     │
│                                                                                          │
│   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐                       │
│   │   SEQUENCING    │   │  STATE MGMT     │   │ NODE INTERACTION│                       │
│   │                 │   │                 │   │                 │                       │
│   │ What order do   │   │ Track progress  │   │ How agents      │                       │
│   │ agents run?     │   │ across steps    │   │ communicate     │                       │
│   │                 │   │                 │   │                 │                       │
│   │ • Sequential    │   │ • Checkpoints   │   │ • Message pass  │                       │
│   │ • Parallel      │   │ • Persistence   │   │ • Shared state  │                       │
│   │ • Conditional   │   │ • Recovery      │   │ • Event-driven  │                       │
│   └─────────────────┘   └─────────────────┘   └─────────────────┘                       │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Workflow Patterns for Population Advyzer

### Pattern 1: DAG-Based Workflow (Current Databricks Style)

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    RISK ADJUSTMENT WORKFLOW DAG                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘

                              ┌─────────────┐
                              │   START     │
                              └──────┬──────┘
                                     │
                              ┌──────▼──────┐
                              │ Data Loader │
                              └──────┬──────┘
                                     │
                    ┌────────────────┼────────────────┐
                    │                │                │
             ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
             │ Transform   │  │ MOR Health  │  │ Qualifying  │
             │ Common      │  │ Events      │  │ Claims      │
             └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
                    │                │                │
                    └────────────────┼────────────────┘
                                     │
                              ┌──────▼──────┐
                              │ MA Model    │
                              │ Input Data  │
                              └──────┬──────┘
                                     │
                    ┌────────────────┼────────────────┐
                    │                │                │
             ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
             │ CMS-HCC     │  │ CMS-RxHCC   │  │ CMS-ESRD    │
             │ Scoring     │  │ Scoring     │  │ Scoring     │
             └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
                    │                │                │
                    └────────────────┼────────────────┘
                                     │
                              ┌──────▼──────┐
                              │ Gap         │
                              │ Suspecting  │
                              └──────┬──────┘
                                     │
                              ┌──────▼──────┐
                              │    END      │
                              └─────────────┘
```

### Pattern 2: State Machine for Agent Workflows

```python
from enum import Enum
from typing import TypedDict

class WorkflowState(Enum):
    INIT = "init"
    DATA_LOADING = "data_loading"
    DATA_VALIDATED = "data_validated"
    SCORING = "scoring"
    SCORES_COMPLETE = "scores_complete"
    GAP_ANALYSIS = "gap_analysis"
    GAPS_COMPLETE = "gaps_complete"
    PERSISTING = "persisting"
    COMPLETE = "complete"
    ERROR = "error"

class RiskAdjustmentState(TypedDict):
    # Workflow state
    current_state: WorkflowState
    previous_states: list[WorkflowState]
    
    # Input parameters
    plan_name: str
    risk_year: int
    member_bids: list[str]
    
    # Intermediate results (persisted for recovery)
    data_quality_result: dict | None
    risk_scores: dict | None
    gaps_identified: list | None
    
    # Tracking
    started_at: datetime
    checkpoints: list[dict]
    errors: list[str]

class WorkflowStateMachine:
    """
    State machine governing workflow transitions.
    """
    
    VALID_TRANSITIONS = {
        WorkflowState.INIT: [WorkflowState.DATA_LOADING],
        WorkflowState.DATA_LOADING: [WorkflowState.DATA_VALIDATED, WorkflowState.ERROR],
        WorkflowState.DATA_VALIDATED: [WorkflowState.SCORING],
        WorkflowState.SCORING: [WorkflowState.SCORES_COMPLETE, WorkflowState.ERROR],
        WorkflowState.SCORES_COMPLETE: [WorkflowState.GAP_ANALYSIS],
        WorkflowState.GAP_ANALYSIS: [WorkflowState.GAPS_COMPLETE, WorkflowState.ERROR],
        WorkflowState.GAPS_COMPLETE: [WorkflowState.PERSISTING],
        WorkflowState.PERSISTING: [WorkflowState.COMPLETE, WorkflowState.ERROR],
    }
    
    def transition(self, state: RiskAdjustmentState, new_state: WorkflowState) -> RiskAdjustmentState:
        current = state["current_state"]
        
        if new_state not in self.VALID_TRANSITIONS.get(current, []):
            raise InvalidTransition(f"Cannot go from {current} to {new_state}")
        
        # Checkpoint before transition
        state["checkpoints"].append({
            "from": current,
            "to": new_state,
            "timestamp": datetime.now(),
            "snapshot": self._snapshot_state(state)
        })
        
        state["previous_states"].append(current)
        state["current_state"] = new_state
        
        return state
```

---

## 3. State Persistence and Recovery

```python
class WorkflowCheckpointer:
    """
    Persist workflow state for recovery from failures.
    """
    
    def __init__(self, storage_path: str):
        self.storage_path = storage_path  # Delta table or Redis
    
    async def checkpoint(self, workflow_id: str, state: RiskAdjustmentState):
        """Save current state to durable storage"""
        checkpoint = {
            "workflow_id": workflow_id,
            "state": state,
            "timestamp": datetime.now().isoformat(),
            "version": self._get_next_version(workflow_id)
        }
        
        # Write to Delta table for durability
        await self._write_checkpoint(checkpoint)
        
    async def recover(self, workflow_id: str) -> RiskAdjustmentState | None:
        """Recover latest state from storage"""
        checkpoint = await self._read_latest_checkpoint(workflow_id)
        
        if checkpoint:
            logger.info(f"Recovering workflow {workflow_id} from state {checkpoint['state']['current_state']}")
            return checkpoint["state"]
        
        return None
    
    async def replay_from_checkpoint(self, workflow_id: str, checkpoint_version: int):
        """Replay workflow from a specific checkpoint"""
        checkpoint = await self._read_checkpoint(workflow_id, checkpoint_version)
        return checkpoint["state"]
```

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    CHECKPOINT AND RECOVERY FLOW                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘

  Normal Flow:
  ┌──────┐     ┌──────┐     ┌──────┐     ┌──────┐     ┌──────┐
  │ Step │ ──▶ │ Step │ ──▶ │ Step │ ──▶ │ Step │ ──▶ │ Step │
  │  1   │     │  2   │     │  3   │     │  4   │     │  5   │
  └──┬───┘     └──┬───┘     └──┬───┘     └──┬───┘     └──────┘
     │            │            │            │
     ▼            ▼            ▼            ▼
  ┌──────────────────────────────────────────────┐
  │           CHECKPOINT STORAGE                 │
  │  (Delta Table / Redis / Cosmos DB)           │
  └──────────────────────────────────────────────┘

  Recovery Flow:
  ┌──────┐     ┌──────┐     ┌──────┐
  │ Step │ ──▶ │ Step │ ──▶ │ FAIL │
  │  1   │     │  2   │     │  ✗   │
  └──────┘     └──────┘     └──────┘
                               │
                               ▼ Load checkpoint v2
  ┌──────────────────────────────────────────────┐
  │           CHECKPOINT STORAGE                 │
  └──────────────────────────────────────────────┘
                               │
                               ▼
                           ┌──────┐     ┌──────┐     ┌──────┐
                           │ Step │ ──▶ │ Step │ ──▶ │ Step │
                           │  3   │     │  4   │     │  5   │
                           └──────┘     └──────┘     └──────┘
```

---

## 4. Interview Answer Framework

### **1. Define (30 seconds)**
> "Workflow management for agents involves controlling sequencing (what order agents run), state management (tracking progress and enabling recovery), and node interactions (how agents communicate and share data)."

### **2. Implementation (1 minute)**
> "I design workflows as state machines with explicit valid transitions. Each state change creates a checkpoint to durable storage - Delta tables in our case. This enables recovery from any failure point without reprocessing completed steps.
>
> For sequencing, I use DAG-based orchestration where parallel branches (like scoring three risk models simultaneously) converge before downstream steps."

### **3. Population Advyzer Example (30 seconds)**
> "In our risk adjustment pipeline, the workflow flows: Data Loading → Validation → Parallel Scoring (HCC, RxHCC, ESRD) → Gap Analysis → Persistence. State is checkpointed after each major phase, so if gap analysis fails, we recover from the 'scores_complete' checkpoint rather than restarting from scratch."

---

---

# Question 5: Architect Meta-Agent (Agents of Agents) hierarchies for complex, layered autonomous decision-making

---

## 1. What is a Meta-Agent?

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                              META-AGENT CONCEPT                                          │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   A Meta-Agent is an "Agent of Agents" - it doesn't do work directly but               │
│   orchestrates, supervises, and coordinates other agents.                               │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                          META-AGENT (Level 2)                                    │   │
│   │                                                                                  │   │
│   │   • Receives high-level goals                                                   │   │
│   │   • Decomposes into sub-goals                                                   │   │
│   │   • Assigns to specialist agents                                                │   │
│   │   • Monitors progress                                                           │   │
│   │   • Handles conflicts/failures                                                  │   │
│   │   • Aggregates results                                                          │   │
│   │                                                                                  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                         │                                                │
│              ┌──────────────────────────┼──────────────────────────┐                    │
│              │                          │                          │                    │
│              ▼                          ▼                          ▼                    │
│   ┌─────────────────┐        ┌─────────────────┐        ┌─────────────────┐            │
│   │  Worker Agent 1 │        │  Worker Agent 2 │        │  Worker Agent 3 │            │
│   │  (Specialist)   │        │  (Specialist)   │        │  (Specialist)   │            │
│   └─────────────────┘        └─────────────────┘        └─────────────────┘            │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Multi-Level Hierarchy for Population Advyzer

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    THREE-LEVEL AGENT HIERARCHY                                           │
└──────────────────────────────────────────────────────────────────────────────────────────┘

LEVEL 3 (Strategic):
                              ┌─────────────────────────────┐
                              │   EXECUTIVE META-AGENT      │
                              │                             │
                              │ Goal: "Maximize RAF accuracy│
                              │        while minimizing     │
                              │        compliance risk"     │
                              └──────────────┬──────────────┘
                                             │
         ┌───────────────────────────────────┼───────────────────────────────────┐
         │                                   │                                   │
         ▼                                   ▼                                   ▼

LEVEL 2 (Tactical):
┌─────────────────────┐      ┌─────────────────────┐      ┌─────────────────────┐
│  RISK SCORING       │      │  GAP CLOSURE        │      │  COMPLIANCE         │
│  META-AGENT         │      │  META-AGENT         │      │  META-AGENT         │
│                     │      │                     │      │                     │
│ "Optimize scoring   │      │ "Identify & close   │      │ "Ensure audit       │
│  accuracy"          │      │  documentation gaps"│      │  readiness"         │
└──────────┬──────────┘      └──────────┬──────────┘      └──────────┬──────────┘
           │                            │                            │
     ┌─────┼─────┐                ┌─────┼─────┐                ┌─────┼─────┐
     │     │     │                │     │     │                │     │     │
     ▼     ▼     ▼                ▼     ▼     ▼                ▼     ▼     ▼

LEVEL 1 (Operational):
┌─────┐ ┌─────┐ ┌─────┐    ┌─────┐ ┌─────┐ ┌─────┐    ┌─────┐ ┌─────┐ ┌─────┐
│HCC  │ │RxHCC│ │ESRD │    │Rx   │ │Dx   │ │Proc │    │Audit│ │Data │ │Doc  │
│Score│ │Score│ │Score│    │Gap  │ │Gap  │ │Gap  │    │Trail│ │Valid│ │Gen  │
│Agent│ │Agent│ │Agent│    │Agent│ │Agent│ │Agent│    │Agent│ │Agent│ │Agent│
└─────┘ └─────┘ └─────┘    └─────┘ └─────┘ └─────┘    └─────┘ └─────┘ └─────┘
```

---

## 3. Meta-Agent Implementation

```python
class MetaAgent:
    """
    Agent of Agents - orchestrates multiple worker agents.
    """
    
    def __init__(self, name: str, worker_agents: list[Agent]):
        self.name = name
        self.workers = {agent.name: agent for agent in worker_agents}
        self.active_tasks = {}
    
    async def run(self, goal: str, context: dict) -> MetaAgentResult:
        """
        High-level orchestration:
        1. Decompose goal into sub-goals
        2. Assign to appropriate workers
        3. Monitor and coordinate
        4. Handle failures
        5. Aggregate results
        """
        
        # Step 1: Decompose goal using LLM
        sub_goals = await self._decompose_goal(goal, context)
        logger.info(f"Decomposed into {len(sub_goals)} sub-goals")
        
        # Step 2: Plan execution (parallel vs sequential)
        execution_plan = await self._plan_execution(sub_goals)
        
        # Step 3: Execute with monitoring
        results = []
        for phase in execution_plan.phases:
            if phase.parallel:
                phase_results = await asyncio.gather(*[
                    self._execute_with_monitoring(task)
                    for task in phase.tasks
                ])
            else:
                phase_results = []
                for task in phase.tasks:
                    result = await self._execute_with_monitoring(task)
                    phase_results.append(result)
            
            results.extend(phase_results)
            
            # Check for conflicts or failures
            if any(r.failed for r in phase_results):
                await self._handle_phase_failure(phase, phase_results)
        
        # Step 4: Aggregate and synthesize
        final_result = await self._aggregate_results(goal, results)
        
        return final_result
    
    async def _decompose_goal(self, goal: str, context: dict) -> list[SubGoal]:
        """Use LLM to break down high-level goal"""
        
        prompt = f"""
        You are a planning agent. Decompose this goal into specific sub-tasks:
        
        Goal: {goal}
        Context: {context}
        Available Workers: {list(self.workers.keys())}
        
        For each sub-task, specify:
        - task_id
        - description
        - assigned_worker
        - dependencies (other task_ids that must complete first)
        - priority
        """
        
        response = await self.llm.complete(prompt, response_format=SubGoalList)
        return response.sub_goals
    
    async def _handle_phase_failure(self, phase: Phase, results: list):
        """Meta-agent decides how to handle failures"""
        
        failed = [r for r in results if r.failed]
        
        for failure in failed:
            # Option 1: Retry with different approach
            if failure.retryable:
                await self._retry_with_adaptation(failure)
            
            # Option 2: Reassign to different worker
            elif failure.reassignable:
                alternative_worker = self._find_alternative_worker(failure.task)
                await self._reassign_task(failure.task, alternative_worker)
            
            # Option 3: Escalate to human
            else:
                await self._escalate_to_human(failure)
```

---

## 4. Conflict Resolution Between Agents

```python
class ConflictResolver:
    """
    Handles conflicts when multiple agents produce contradictory results.
    """
    
    async def resolve(self, conflict: AgentConflict) -> Resolution:
        """
        Resolution strategies:
        1. Voting (majority wins)
        2. Confidence-weighted (highest confidence wins)
        3. Hierarchy (senior agent wins)
        4. Evidence-based (most evidence wins)
        5. Human escalation
        """
        
        if conflict.type == ConflictType.SCORING_DISAGREEMENT:
            # For risk scores, use confidence-weighted average
            return self._weighted_resolution(conflict.results)
        
        elif conflict.type == ConflictType.GAP_CLASSIFICATION:
            # For gaps, require consensus or escalate
            if self._has_consensus(conflict.results, threshold=0.7):
                return self._majority_vote(conflict.results)
            else:
                return await self._escalate_to_clinical_review(conflict)
        
        elif conflict.type == ConflictType.SUPPRESSION_DECISION:
            # For hierarchy suppression, most conservative wins
            return self._conservative_resolution(conflict.results)
```

---

## 5. Interview Answer Framework

### **1. Define (30 seconds)**
> "A Meta-Agent is an 'Agent of Agents' - it doesn't perform work directly but orchestrates other agents. It decomposes high-level goals, assigns sub-tasks, monitors progress, handles failures, and aggregates results."

### **2. Hierarchy Design (1 minute)**
> "I design three-level hierarchies:
> - **Level 3 (Strategic)**: Executive agent with business-level goals like 'maximize RAF accuracy'
> - **Level 2 (Tactical)**: Domain meta-agents for Risk Scoring, Gap Closure, Compliance
> - **Level 1 (Operational)**: Specialist worker agents doing actual computation
>
> The meta-agent at each level has authority to reassign tasks, resolve conflicts, and escalate issues."

### **3. Population Advyzer Example (30 seconds)**
> "Our Gap Closure Meta-Agent receives the goal 'close documentation gaps for Plan X'. It decomposes this into sub-goals for Pharmacy Gap Agent, Diagnosis Gap Agent, and Procedure Gap Agent. If agents disagree on whether a gap is valid, the meta-agent applies consensus rules or escalates to clinical review."

---

---

# Question 6: Implement A2A (Agent-to-Agent) Protocol standards for secure, structured inter-agent communication

---

## 1. What is A2A Protocol?

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         A2A (AGENT-TO-AGENT) PROTOCOL                                    │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   A standardized way for AI agents to communicate with each other:                      │
│                                                                                          │
│   • Message Format - Structured schema for requests/responses                           │
│   • Discovery - How agents find each other                                              │
│   • Authentication - Verify agent identity                                              │
│   • Authorization - What can each agent access                                          │
│   • Error Handling - Standard error codes and recovery                                  │
│                                                                                          │
│   ┌─────────────┐        A2A Protocol         ┌─────────────┐                          │
│   │   AGENT A   │ ◀──────────────────────────▶ │   AGENT B   │                          │
│   │             │                              │             │                          │
│   │ • Identity  │    ┌──────────────────┐     │ • Identity  │                          │
│   │ • Capabilities    │ • Message Schema │     │ • Capabilities                         │
│   │ • Permissions│    │ • Auth Token     │     │ • Permissions│                         │
│   └─────────────┘    │ • Correlation ID │     └─────────────┘                          │
│                      │ • Timestamp      │                                               │
│                      └──────────────────┘                                               │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. A2A Message Schema

```python
from pydantic import BaseModel
from datetime import datetime
from enum import Enum
from typing import Any

class MessageType(Enum):
    REQUEST = "request"
    RESPONSE = "response"
    EVENT = "event"
    ERROR = "error"

class A2AMessage(BaseModel):
    """
    Standard A2A message format.
    """
    # Header
    message_id: str                    # Unique message ID (UUID)
    correlation_id: str                # Links request/response pairs
    timestamp: datetime                # ISO 8601 timestamp
    message_type: MessageType          # request, response, event, error
    
    # Routing
    sender: AgentIdentity              # Who sent it
    receiver: AgentIdentity            # Who should receive it
    reply_to: str | None               # Queue/topic for response
    
    # Security
    auth_token: str                    # JWT token for authentication
    signature: str                     # Message signature for integrity
    
    # Payload
    action: str                        # What action is requested
    payload: dict[str, Any]            # Action-specific data
    
    # Metadata
    priority: int                      # 1=urgent, 5=low
    ttl_seconds: int                   # Time to live
    trace_context: dict                # Distributed tracing context

class AgentIdentity(BaseModel):
    """
    Agent identification for A2A communication.
    """
    agent_id: str                      # Unique agent identifier
    agent_type: str                    # "risk_scoring", "gap_analysis", etc.
    version: str                       # Agent version
    capabilities: list[str]            # What this agent can do
    
# Example A2A Request
request = A2AMessage(
    message_id="msg-123",
    correlation_id="corr-456",
    timestamp=datetime.now(),
    message_type=MessageType.REQUEST,
    sender=AgentIdentity(
        agent_id="orchestrator-1",
        agent_type="orchestrator",
        version="1.0.0",
        capabilities=["coordinate", "delegate"]
    ),
    receiver=AgentIdentity(
        agent_id="risk-scoring-1",
        agent_type="risk_scoring",
        version="2.1.0",
        capabilities=["hcc_scoring", "rxhcc_scoring"]
    ),
    auth_token="eyJhbG...",
    signature="sha256:abc123...",
    action="calculate_risk_scores",
    payload={
        "member_bids": ["123", "456"],
        "risk_year": 2026,
        "model_version": "V28"
    },
    priority=2,
    ttl_seconds=300,
    trace_context={"trace_id": "abc", "span_id": "def"}
)
```

---

## 3. Agent Discovery and Registry

```python
class AgentRegistry:
    """
    Central registry for agent discovery.
    """
    
    def __init__(self):
        self.agents: dict[str, AgentRegistration] = {}
    
    async def register(self, agent: AgentRegistration):
        """Register an agent with its capabilities"""
        self.agents[agent.agent_id] = agent
        logger.info(f"Registered agent: {agent.agent_id} with capabilities: {agent.capabilities}")
    
    async def discover(self, capability: str) -> list[AgentRegistration]:
        """Find agents with a specific capability"""
        return [
            agent for agent in self.agents.values()
            if capability in agent.capabilities and agent.status == "healthy"
        ]
    
    async def get_agent(self, agent_id: str) -> AgentRegistration | None:
        """Get specific agent by ID"""
        return self.agents.get(agent_id)

class AgentRegistration(BaseModel):
    agent_id: str
    agent_type: str
    capabilities: list[str]
    endpoint: str                      # How to reach this agent
    status: str                        # healthy, degraded, unhealthy
    last_heartbeat: datetime
    metadata: dict
```

---

## 4. Secure Communication

```python
import jwt
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding

class A2ASecurity:
    """
    Security layer for A2A communication.
    """
    
    def __init__(self, private_key, public_keys: dict[str, PublicKey]):
        self.private_key = private_key
        self.public_keys = public_keys  # agent_id -> public_key
    
    def sign_message(self, message: A2AMessage) -> str:
        """Sign message for integrity verification"""
        payload = message.model_dump_json()
        signature = self.private_key.sign(
            payload.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return signature.hex()
    
    def verify_message(self, message: A2AMessage) -> bool:
        """Verify message signature"""
        sender_key = self.public_keys.get(message.sender.agent_id)
        if not sender_key:
            raise UnknownSenderError(f"Unknown sender: {message.sender.agent_id}")
        
        try:
            payload = message.model_dump_json()
            sender_key.verify(
                bytes.fromhex(message.signature),
                payload.encode(),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except InvalidSignature:
            return False
    
    def create_auth_token(self, agent_id: str, permissions: list[str]) -> str:
        """Create JWT token for agent authentication"""
        return jwt.encode(
            {
                "agent_id": agent_id,
                "permissions": permissions,
                "iat": datetime.now(),
                "exp": datetime.now() + timedelta(hours=1)
            },
            self.private_key,
            algorithm="RS256"
        )
```

---

## 5. Interview Answer Framework

### **1. Define (30 seconds)**
> "A2A Protocol is a standard for how AI agents communicate securely. It defines message formats, agent discovery, authentication, and error handling - similar to how REST APIs standardize human-to-machine communication."

### **2. Key Components (1 minute)**
> "My A2A implementation includes:
> - **Structured messages** with headers (routing, auth) and typed payloads
> - **Agent registry** for discovery ('find me an agent that can do HCC scoring')
> - **JWT authentication** so agents verify each other's identity
> - **Message signing** for integrity verification
> - **Correlation IDs** to track request/response pairs across distributed traces"

### **3. Healthcare Security (30 seconds)**
> "For healthcare, A2A security is critical. Agents handling PHI must authenticate before receiving data. We use short-lived JWT tokens, message-level encryption for sensitive payloads, and audit logging of all inter-agent communication for HIPAA compliance."

---

---

# Question 7: Assess and select agentic frameworks (LangGraph, LangChain, AutoGen, Semantic Kernel) based on use-case fit

---

## 1. Framework Comparison

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    AGENTIC FRAMEWORK COMPARISON                                          │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  Framework      │ Strengths                    │ Best For                               │
│  ───────────────┼──────────────────────────────┼────────────────────────────────────────│
│                 │                              │                                        │
│  LangGraph      │ • State machines             │ Complex workflows with                 │
│  (LangChain)    │ • Cycles & loops             │ conditional branching                  │
│                 │ • Checkpointing              │ and state persistence                  │
│                 │ • Human-in-the-loop          │                                        │
│                 │                              │                                        │
│  ───────────────┼──────────────────────────────┼────────────────────────────────────────│
│                 │                              │                                        │
│  LangChain      │ • Rich ecosystem             │ RAG applications,                      │
│                 │ • Many integrations          │ simple chains,                         │
│                 │ • Easy prototyping           │ quick prototypes                       │
│                 │ • LCEL (declarative)         │                                        │
│                 │                              │                                        │
│  ───────────────┼──────────────────────────────┼────────────────────────────────────────│
│                 │                              │                                        │
│  AutoGen        │ • Multi-agent conversations  │ Collaborative problem                  │
│  (Microsoft)    │ • Code execution             │ solving, code generation,              │
│                 │ • Group chat patterns        │ research tasks                         │
│                 │ • Easy agent definition      │                                        │
│                 │                              │                                        │
│  ───────────────┼──────────────────────────────┼────────────────────────────────────────│
│                 │                              │                                        │
│  Semantic       │ • .NET/C# native             │ Enterprise .NET apps,                  │
│  Kernel (MS)    │ • Plugin architecture        │ Microsoft ecosystem,                   │
│                 │ • Planner patterns           │ Copilot extensions                     │
│                 │ • Azure integration          │                                        │
│                 │                              │                                        │
│  ───────────────┼──────────────────────────────┼────────────────────────────────────────│
│                 │                              │                                        │
│  CrewAI         │ • Role-based agents          │ Team simulations,                      │
│                 │ • Process orchestration      │ content generation,                    │
│                 │ • Easy configuration         │ research workflows                     │
│                 │ • Hierarchical crews         │                                        │
│                 │                              │                                        │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Decision Matrix for Population Advyzer

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    FRAMEWORK SELECTION FOR POPULATION ADVYZER                            │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  Requirement                        │ LangGraph │ LangChain │ AutoGen │ Sem.Kernel      │
│  ───────────────────────────────────┼───────────┼───────────┼─────────┼─────────────────│
│  Complex state management           │    ✓✓✓    │     ✓     │   ✓✓    │      ✓✓        │
│  Checkpointing/recovery             │    ✓✓✓    │     ✗     │    ✓    │       ✓        │
│  Multi-agent orchestration          │    ✓✓✓    │     ✓     │  ✓✓✓    │      ✓✓        │
│  Python ecosystem                   │    ✓✓✓    │    ✓✓✓    │  ✓✓✓    │       ✓        │
│  Human-in-the-loop                  │    ✓✓✓    │     ✓     │   ✓✓    │      ✓✓        │
│  Production stability               │    ✓✓     │    ✓✓     │   ✓✓    │     ✓✓✓        │
│  Azure integration                  │    ✓✓     │    ✓✓     │  ✓✓✓    │     ✓✓✓        │
│  Databricks compatibility           │    ✓✓✓    │    ✓✓✓    │   ✓✓    │       ✓        │
│  ───────────────────────────────────┼───────────┼───────────┼─────────┼─────────────────│
│  TOTAL SCORE                        │    21     │    15     │   18    │      16        │
│                                                                                          │
│  RECOMMENDATION: LangGraph for complex workflows, AutoGen for collaborative agents      │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Framework Selection by Use Case

```python
# Decision tree for framework selection

def select_framework(use_case: dict) -> str:
    """
    Select best framework based on use case requirements.
    """
    
    # Complex stateful workflows with recovery needs
    if use_case.get("needs_checkpointing") and use_case.get("complex_state"):
        return "LangGraph"
    
    # Multi-agent collaboration/conversation
    if use_case.get("agent_collaboration") and use_case.get("code_execution"):
        return "AutoGen"
    
    # Microsoft/.NET enterprise environment
    if use_case.get("dotnet_required") or use_case.get("copilot_extension"):
        return "Semantic Kernel"
    
    # Simple RAG or chain-based applications
    if use_case.get("simple_rag") or use_case.get("quick_prototype"):
        return "LangChain"
    
    # Role-based team simulation
    if use_case.get("team_simulation") or use_case.get("content_generation"):
        return "CrewAI"
    
    # Default for most production use cases
    return "LangGraph"

# Population Advyzer Use Cases
use_cases = {
    "risk_scoring_pipeline": {
        "needs_checkpointing": True,
        "complex_state": True,
        "agent_collaboration": True,
        "recommendation": "LangGraph"
    },
    "gap_analysis_agents": {
        "agent_collaboration": True,
        "needs_consensus": True,
        "recommendation": "LangGraph + AutoGen patterns"
    },
    "clinical_rag": {
        "simple_rag": True,
        "recommendation": "LangChain"
    },
    "conversational_bi": {
        "agent_collaboration": True,
        "code_execution": True,
        "recommendation": "AutoGen"
    }
}
```

---

## 4. Interview Answer Framework

### **1. Framework Knowledge (30 seconds)**
> "I evaluate frameworks based on: state management, checkpointing, multi-agent support, ecosystem maturity, and enterprise readiness. The main options are LangGraph, LangChain, AutoGen, and Semantic Kernel."

### **2. Selection Criteria (1 minute)**
> "For complex healthcare workflows like risk adjustment:
> - **LangGraph** is my primary choice - excellent state machines, checkpointing, and recovery
> - **AutoGen** for collaborative reasoning (gap analysis with multiple expert agents)
> - **LangChain** for simpler RAG components (clinical knowledge retrieval)
> - **Semantic Kernel** if the organization is .NET-centric"

### **3. Population Advyzer Choice (30 seconds)**
> "For our risk adjustment platform, I chose LangGraph because we need stateful workflows with checkpointing (recovering from failures mid-pipeline), conditional branching (different paths based on data quality), and human-in-the-loop (clinical review for high-stakes gap decisions)."

---

---

# Question 8: Govern MCP Hub architecture, defining policies and standards across a centralized pool of MCP Servers

---

## 1. What is MCP (Model Context Protocol)?

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    MCP (MODEL CONTEXT PROTOCOL)                                          │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   MCP is a protocol for connecting AI models to external tools and data sources:        │
│                                                                                          │
│   ┌─────────────────┐          MCP Protocol          ┌─────────────────┐               │
│   │                 │ ◀────────────────────────────▶ │                 │               │
│   │    AI MODEL     │                                │   MCP SERVER    │               │
│   │   (Claude,      │    • Tool definitions          │   (Data Source, │               │
│   │    GPT, etc.)   │    • Resource access           │    API, DB)     │               │
│   │                 │    • Prompt templates          │                 │               │
│   └─────────────────┘                                └─────────────────┘               │
│                                                                                          │
│   Key Concepts:                                                                          │
│   • MCP Server: Exposes tools/resources via standard protocol                           │
│   • MCP Client: AI application that connects to servers                                 │
│   • MCP Hub: Central registry/gateway for multiple servers                              │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. MCP Hub Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    ENTERPRISE MCP HUB ARCHITECTURE                                       │
└──────────────────────────────────────────────────────────────────────────────────────────┘

                              ┌─────────────────────────────┐
                              │         MCP HUB             │
                              │     (Central Gateway)       │
                              │                             │
                              │  • Server Registry          │
                              │  • Policy Enforcement       │
                              │  • Authentication           │
                              │  • Rate Limiting            │
                              │  • Audit Logging            │
                              └──────────────┬──────────────┘
                                             │
         ┌───────────────────────────────────┼───────────────────────────────────┐
         │                                   │                                   │
         ▼                                   ▼                                   ▼
┌─────────────────┐              ┌─────────────────┐              ┌─────────────────┐
│  MCP SERVER:    │              │  MCP SERVER:    │              │  MCP SERVER:    │
│  Databricks     │              │  Clinical KB    │              │  CMS Reference  │
│                 │              │                 │              │                 │
│  Tools:         │              │  Tools:         │              │  Tools:         │
│  • sql_query    │              │  • search_docs  │              │  • icd_lookup   │
│  • read_table   │              │  • get_guideline│              │  • hcc_mapping  │
│  • write_table  │              │  • cite_source  │              │  • coefficient  │
│                 │              │                 │              │                 │
│  Resources:     │              │  Resources:     │              │  Resources:     │
│  • Unity Catalog│              │  • CMS PDFs     │              │  • ref_tables   │
│  • Delta Tables │              │  • Clinical Docs│              │  • ICD-10 codes │
└─────────────────┘              └─────────────────┘              └─────────────────┘
         │                                   │                                   │
         └───────────────────────────────────┼───────────────────────────────────┘
                                             │
                                             ▼
                              ┌─────────────────────────────┐
                              │       AI AGENTS             │
                              │                             │
                              │  • Risk Scoring Agent       │
                              │  • Gap Analysis Agent       │
                              │  • Clinical Reasoning Agent │
                              └─────────────────────────────┘
```

---

## 3. MCP Hub Governance Policies

```python
class MCPHubGovernance:
    """
    Governance policies for MCP Hub.
    """
    
    # Server Registration Policy
    SERVER_REGISTRATION_POLICY = {
        "required_metadata": [
            "owner_team",
            "data_classification",      # public, internal, confidential, phi
            "sla_tier",                  # gold, silver, bronze
            "compliance_requirements"   # hipaa, sox, pci
        ],
        "approval_required_for": [
            "phi_data_access",
            "write_operations",
            "external_network_access"
        ],
        "auto_approved_for": [
            "read_only_public_data",
            "reference_data_access"
        ]
    }
    
    # Access Control Policy
    ACCESS_CONTROL_POLICY = {
        "agent_permissions": {
            "risk_scoring_agent": {
                "allowed_servers": ["databricks", "cms_reference"],
                "allowed_tools": ["sql_query", "read_table", "icd_lookup", "hcc_mapping"],
                "denied_tools": ["write_table", "delete_table"],
                "data_classification_max": "phi"
            },
            "gap_analysis_agent": {
                "allowed_servers": ["databricks", "clinical_kb", "cms_reference"],
                "allowed_tools": ["*"],
                "denied_tools": ["write_table"],
                "data_classification_max": "phi"
            }
        }
    }
    
    # Rate Limiting Policy
    RATE_LIMITS = {
        "default": {"requests_per_minute": 100, "tokens_per_minute": 50000},
        "gold_tier": {"requests_per_minute": 1000, "tokens_per_minute": 500000},
        "silver_tier": {"requests_per_minute": 500, "tokens_per_minute": 200000},
        "bronze_tier": {"requests_per_minute": 100, "tokens_per_minute": 50000}
    }
    
    # Audit Policy
    AUDIT_POLICY = {
        "log_all_requests": True,
        "log_phi_access": True,
        "retention_days": 365,
        "alert_on": [
            "phi_access_outside_business_hours",
            "bulk_data_extraction",
            "repeated_auth_failures"
        ]
    }
```

---

## 4. Interview Answer Framework

### **1. Define (30 seconds)**
> "MCP Hub is a centralized gateway for managing multiple MCP Servers - the tools and data sources that AI agents connect to. The Hub provides discovery, authentication, policy enforcement, and audit logging."

### **2. Governance (1 minute)**
> "I govern MCP Hubs with four policy layers:
> - **Registration policies**: Required metadata, approval workflows for sensitive servers
> - **Access control**: Which agents can use which servers/tools, data classification limits
> - **Rate limiting**: Tiered limits based on SLA (gold/silver/bronze)
> - **Audit policies**: Log all access, alert on anomalies, PHI access tracking"

### **3. Healthcare Example (30 seconds)**
> "For our risk adjustment agents, the MCP Hub governs access to Databricks (PHI data), Clinical KB (guidelines), and CMS Reference (ICD codes). Risk Scoring Agent can read but not write to Databricks. All PHI access is logged with 365-day retention for HIPAA compliance."

---

---

# Question 9: Define MCP Server boundaries, responsibilities, and segregation strategies within the enterprise hub

---

## 1. MCP Server Segregation Strategies

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    MCP SERVER SEGREGATION                                                │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   SEGREGATION BY:                                                                        │
│                                                                                          │
│   1. DATA CLASSIFICATION                                                                 │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                   │
│   │   PUBLIC    │  │  INTERNAL   │  │CONFIDENTIAL │  │     PHI     │                   │
│   │   Server    │  │   Server    │  │   Server    │  │   Server    │                   │
│   │             │  │             │  │             │  │             │                   │
│   │ • CMS refs  │  │ • Analytics │  │ • Finance   │  │ • Claims    │                   │
│   │ • ICD codes │  │ • Reports   │  │ • Contracts │  │ • Members   │                   │
│   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘                   │
│                                                                                          │
│   2. FUNCTION/DOMAIN                                                                     │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                   │
│   │    DATA     │  │  CLINICAL   │  │  ANALYTICS  │  │   ADMIN     │                   │
│   │   Server    │  │   Server    │  │   Server    │  │   Server    │                   │
│   │             │  │             │  │             │  │             │                   │
│   │ • CRUD ops  │  │ • KB search │  │ • Dashboards│  │ • User mgmt │                   │
│   │ • ETL tools │  │ • Guidelines│  │ • Reports   │  │ • Configs   │                   │
│   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘                   │
│                                                                                          │
│   3. ENVIRONMENT                                                                         │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                   │
│   │     DEV     │  │     QA      │  │     STG     │  │    PROD     │                   │
│   │   Server    │  │   Server    │  │   Server    │  │   Server    │                   │
│   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘                   │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Server Responsibility Matrix

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    MCP SERVER RESPONSIBILITY MATRIX                                      │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  Server              │ Responsibilities           │ Boundaries (NOT responsible for)    │
│  ────────────────────┼────────────────────────────┼─────────────────────────────────────│
│                      │                            │                                     │
│  Databricks MCP      │ • SQL query execution      │ • Data transformation logic         │
│                      │ • Table read/write         │ • Business rules                    │
│                      │ • Schema introspection     │ • Data quality validation           │
│                      │ • Catalog navigation       │ • Cross-environment access          │
│                      │                            │                                     │
│  ────────────────────┼────────────────────────────┼─────────────────────────────────────│
│                      │                            │                                     │
│  Clinical KB MCP     │ • Document search          │ • Clinical decision making          │
│                      │ • Guideline retrieval      │ • Diagnosis                         │
│                      │ • Citation generation      │ • Treatment recommendations         │
│                      │ • Source attribution       │ • PHI storage                       │
│                      │                            │                                     │
│  ────────────────────┼────────────────────────────┼─────────────────────────────────────│
│                      │                            │                                     │
│  CMS Reference MCP   │ • ICD-10 lookups           │ • Claim adjudication                │
│                      │ • HCC mappings             │ • Payment calculation               │
│                      │ • Coefficient retrieval    │ • Member data                       │
│                      │ • Version management       │ • Real-time scoring                 │
│                      │                            │                                     │
│  ────────────────────┼────────────────────────────┼─────────────────────────────────────│
│                      │                            │                                     │
│  Snowflake MCP       │ • Dashboard data queries   │ • Transactional writes              │
│                      │ • Aggregation views        │ • Real-time data                    │
│                      │ • Historical analysis      │ • PHI without masking               │
│                      │                            │                                     │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Interview Answer Framework

### **1. Define (30 seconds)**
> "MCP Server boundaries define what each server is responsible for and what it explicitly doesn't handle. This prevents scope creep, ensures single responsibility, and enables independent scaling/security."

### **2. Segregation Strategies (1 minute)**
> "I segregate servers by three dimensions:
> - **Data classification**: Separate servers for PHI vs public data, with different security controls
> - **Function**: Data servers, Clinical KB servers, Analytics servers - each with clear responsibility
> - **Environment**: Dev/QA/STG/Prod isolation to prevent cross-environment data leaks"

### **3. Healthcare Example (30 seconds)**
> "Our Databricks MCP Server handles SQL queries and table access, but NOT business logic like HCC scoring. The Clinical KB Server retrieves guidelines but never makes diagnoses. This separation ensures we can audit exactly what each server touched when reviewing PHI access."

---

---

# Question 10: Advise decision criteria for MCP vs. Azure APIM based on integration patterns and governance needs

---

## 1. MCP vs Azure APIM Comparison

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    MCP vs AZURE APIM COMPARISON                                          │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  Aspect              │ MCP                          │ Azure APIM                         │
│  ────────────────────┼──────────────────────────────┼────────────────────────────────────│
│                      │                              │                                    │
│  Primary Purpose     │ AI-to-Tool communication     │ API management & gateway           │
│                      │                              │                                    │
│  Protocol            │ MCP (JSON-RPC over stdio/SSE)│ REST, GraphQL, WebSocket           │
│                      │                              │                                    │
│  Client              │ AI models (Claude, GPT)      │ Any HTTP client                    │
│                      │                              │                                    │
│  Schema              │ Tool/Resource definitions    │ OpenAPI/Swagger                    │
│                      │                              │                                    │
│  Discovery           │ Dynamic tool discovery       │ Developer portal                   │
│                      │                              │                                    │
│  Auth                │ Token-based, per-server      │ OAuth2, API keys, certificates     │
│                      │                              │                                    │
│  Rate Limiting       │ Basic (per-client)           │ Advanced (quotas, throttling)      │
│                      │                              │                                    │
│  Caching             │ Client-side                  │ Built-in response caching          │
│                      │                              │                                    │
│  Transformation      │ None (pass-through)          │ Request/response policies          │
│                      │                              │                                    │
│  Monitoring          │ Basic logging                │ Azure Monitor, App Insights        │
│                      │                              │                                    │
│  Enterprise Features │ Emerging                     │ Mature (VNet, WAF, etc.)           │
│                      │                              │                                    │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Decision Framework

```python
def choose_mcp_or_apim(requirements: dict) -> str:
    """
    Decision framework for MCP vs Azure APIM.
    """
    
    # Definitely MCP
    if requirements.get("ai_native_tools"):
        # Tools designed for AI models to call
        return "MCP"
    
    if requirements.get("dynamic_tool_discovery"):
        # AI needs to discover available tools at runtime
        return "MCP"
    
    if requirements.get("bidirectional_streaming"):
        # Real-time agent-tool interaction
        return "MCP"
    
    # Definitely APIM
    if requirements.get("external_api_exposure"):
        # Exposing APIs to external consumers
        return "Azure APIM"
    
    if requirements.get("advanced_rate_limiting"):
        # Complex quota management
        return "Azure APIM"
    
    if requirements.get("api_versioning"):
        # Multiple API versions with routing
        return "Azure APIM"
    
    if requirements.get("developer_portal"):
        # Self-service API documentation
        return "Azure APIM"
    
    # Hybrid: Use both
    if requirements.get("ai_agents") and requirements.get("external_consumers"):
        return "Hybrid: MCP for agents, APIM for external"
    
    return "Evaluate case-by-case"
```

---

## 3. Hybrid Architecture for Population Advyzer

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    HYBRID MCP + APIM ARCHITECTURE                                        │
└──────────────────────────────────────────────────────────────────────────────────────────┘

  INTERNAL (AI Agents)                              EXTERNAL (APIs)
  ────────────────────                              ────────────────

  ┌─────────────────┐                              ┌─────────────────┐
  │   AI AGENTS     │                              │ EXTERNAL APPS   │
  │                 │                              │                 │
  │ • Risk Scoring  │                              │ • Partner Apps  │
  │ • Gap Analysis  │                              │ • Mobile Apps   │
  │ • Clinical RAG  │                              │ • Tableau       │
  └────────┬────────┘                              └────────┬────────┘
           │                                                │
           │ MCP Protocol                                   │ REST/GraphQL
           │                                                │
           ▼                                                ▼
  ┌─────────────────┐                              ┌─────────────────┐
  │    MCP HUB      │                              │  AZURE APIM     │
  │                 │                              │                 │
  │ • Tool discovery│                              │ • Rate limiting │
  │ • Agent auth    │                              │ • Versioning    │
  │ • Audit logging │                              │ • Dev portal    │
  └────────┬────────┘                              └────────┬────────┘
           │                                                │
           └────────────────────┬───────────────────────────┘
                                │
                                ▼
                    ┌─────────────────────┐
                    │   BACKEND SERVICES  │
                    │                     │
                    │ • Databricks        │
                    │ • Snowflake         │
                    │ • Clinical KB       │
                    └─────────────────────┘
```

---

## 4. Interview Answer Framework

### **1. Key Difference (30 seconds)**
> "MCP is designed for AI-to-tool communication with dynamic discovery, while APIM is a general-purpose API gateway for any HTTP client. They solve different problems."

### **2. Decision Criteria (1 minute)**
> "Use **MCP** when:
> - AI agents need to discover and call tools dynamically
> - You need bidirectional streaming between agent and tools
> - Tool schemas are designed for LLM consumption
>
> Use **APIM** when:
> - Exposing APIs to external consumers
> - You need advanced rate limiting, quotas, caching
> - API versioning and developer portal are required"

### **3. Recommendation (30 seconds)**
> "For Population Advyzer, I recommend a hybrid: MCP Hub for internal AI agents (risk scoring, gap analysis), and APIM for external API consumers (partner integrations, Tableau). Both connect to the same backend services but with different governance appropriate to their consumers."

---

---

# Question 11: Evaluate and select OCR solutions (Azure Document Intelligence vs. John Snow Labs) aligned to accuracy and scale requirements

---

## 1. OCR Solution Comparison

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    OCR SOLUTION COMPARISON                                               │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  Aspect                  │ Azure Document Intelligence  │ John Snow Labs (Spark OCR)    │
│  ────────────────────────┼──────────────────────────────┼───────────────────────────────│
│                          │                              │                               │
│  Deployment              │ Cloud (Azure) only           │ On-prem, cloud, Databricks    │
│                          │                              │                               │
│  Healthcare Focus        │ General + Healthcare prebuilt│ Healthcare-specialized        │
│                          │                              │ (clinical notes, Rx, labs)    │
│                          │                              │                               │
│  Pre-built Models        │ • Invoice, Receipt, ID       │ • Clinical NER                │
│                          │ • Health Insurance Card      │ • De-identification           │
│                          │ • Custom models              │ • Drug labels                 │
│                          │                              │ • Lab reports                 │
│                          │                              │                               │
│  PHI Handling            │ Data leaves to Azure         │ Can run fully on-prem         │
│                          │                              │                               │
│  Accuracy (Healthcare)   │ ~90-95% (general docs)       │ ~95-98% (clinical docs)       │
│                          │                              │                               │
│  Scale                   │ Managed auto-scaling         │ Spark-based (horizontal)      │
│                          │                              │                               │
│  Integration             │ REST API, SDKs               │ Spark, Databricks native      │
│                          │                              │                               │
│  Cost Model              │ Per page processed           │ License + compute             │
│                          │                              │                               │
│  Customization           │ Custom models (limited)      │ Full model fine-tuning        │
│                          │                              │                               │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Decision Matrix for Population Advyzer

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    OCR SELECTION FOR POPULATION ADVYZER                                  │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  Use Case                       │ Recommendation        │ Reason                         │
│  ───────────────────────────────┼───────────────────────┼────────────────────────────────│
│                                 │                       │                                │
│  Clinical Notes (Unstructured)  │ John Snow Labs        │ Higher accuracy for clinical   │
│                                 │                       │ terminology, runs on Databricks│
│                                 │                       │                                │
│  Health Insurance Cards         │ Azure Doc Intelligence│ Pre-built model, simple API    │
│                                 │                       │                                │
│  Lab Reports                    │ John Snow Labs        │ Lab-specific NER models        │
│                                 │                       │                                │
│  Prescription/Rx Documents      │ John Snow Labs        │ Drug NER, dosage extraction    │
│                                 │                       │                                │
│  Standard Forms (Claims, EOBs)  │ Azure Doc Intelligence│ Good general form extraction   │
│                                 │                       │                                │
│  PHI-Sensitive (On-prem only)   │ John Snow Labs        │ No data leaves the environment │
│                                 │                       │                                │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Interview Answer Framework

### **1. Key Factors (30 seconds)**
> "For OCR selection, I evaluate: accuracy for the document type, PHI handling requirements, deployment flexibility, and integration with existing stack."

### **2. Comparison (1 minute)**
> "**Azure Document Intelligence** is excellent for general documents with good pre-built models and managed scaling. **John Snow Labs Spark OCR** is healthcare-specialized with higher accuracy for clinical documents, runs natively on Databricks, and can be fully on-premise for PHI compliance.
>
> For healthcare, John Snow Labs typically achieves 95-98% accuracy on clinical notes vs. 90-95% for Azure."

### **3. Recommendation (30 seconds)**
> "For Population Advyzer, I'd recommend **John Snow Labs** because: (1) it runs natively on our Databricks stack, (2) higher accuracy on clinical terminology, (3) PHI stays within our environment. Azure Doc Intelligence is a good addition for standard forms like insurance cards."

---

---

# Question 12: Design and optimize OCR pre-processing and post-processing pipelines for healthcare document ingestion

---

## 1. OCR Pipeline Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    HEALTHCARE OCR PIPELINE                                               │
└──────────────────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────────────────────────┐
  │                              PRE-PROCESSING                                            │
  └─────────────────────────────────────────────────────────────────────────────────────────┘

  ┌───────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐
  │  Ingest   │ → │  Classify │ → │  Enhance  │ → │   Rotate  │ → │  Segment  │
  │  Document │   │  Doc Type │   │  Image    │   │   Deskew  │   │  Regions  │
  └───────────┘   └───────────┘   └───────────┘   └───────────┘   └───────────┘
       │                │               │               │               │
       ▼                ▼               ▼               ▼               ▼
  PDF, Images     Clinical Note    Denoise,        Auto-rotate     Header, Body,
  TIFF, Fax       Lab Report       Binarize        Straighten      Table, Footer


  ┌─────────────────────────────────────────────────────────────────────────────────────────┐
  │                              OCR ENGINE                                                │
  └─────────────────────────────────────────────────────────────────────────────────────────┘

  ┌───────────────────────────────────────────────────────────────────────────────────────┐
  │   John Snow Labs Spark OCR / Azure Document Intelligence                              │
  │                                                                                       │
  │   • Character Recognition                                                             │
  │   • Layout Analysis                                                                   │
  │   • Table Extraction                                                                  │
  │   • Confidence Scores                                                                 │
  └───────────────────────────────────────────────────────────────────────────────────────┘


  ┌─────────────────────────────────────────────────────────────────────────────────────────┐
  │                              POST-PROCESSING                                           │
  └─────────────────────────────────────────────────────────────────────────────────────────┘

  ┌───────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐
  │  Spell    │ → │  Clinical │ → │    NER    │ → │ De-Ident  │ → │  Quality  │
  │  Correct  │   │  Normalize│   │ Extraction│   │   PHI     │   │   Score   │
  └───────────┘   └───────────┘   └───────────┘   └───────────┘   └───────────┘
       │                │               │               │               │
       ▼                ▼               ▼               ▼               ▼
  Medical Dict    Abbreviations    ICD codes,      Mask SSN,       0-100 score
  Corrections     Expansion        Drugs, Dates    Names, MRN      for review
```

---

## 2. Pre-Processing Implementation

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from PIL import Image
import cv2
import numpy as np

class OCRPreProcessor:
    """
    Pre-processing pipeline for healthcare documents.
    """
    
    def preprocess(self, df: DataFrame) -> DataFrame:
        """
        Full pre-processing pipeline.
        """
        return (
            df
            .transform(self.classify_document_type)
            .transform(self.enhance_image_quality)
            .transform(self.correct_orientation)
            .transform(self.segment_regions)
        )
    
    def classify_document_type(self, df: DataFrame) -> DataFrame:
        """
        Classify document type to route to appropriate OCR model.
        """
        @udf("string")
        def classify(content: bytes) -> str:
            # Use pre-trained classifier or rules
            features = self._extract_features(content)
            
            if features.has_lab_headers:
                return "lab_report"
            elif features.has_rx_symbol:
                return "prescription"
            elif features.has_insurance_fields:
                return "insurance_card"
            else:
                return "clinical_note"
        
        return df.withColumn("doc_type", classify(col("content")))
    
    def enhance_image_quality(self, df: DataFrame) -> DataFrame:
        """
        Image enhancement for better OCR accuracy.
        """
        @udf("binary")
        def enhance(content: bytes) -> bytes:
            img = cv2.imdecode(np.frombuffer(content, np.uint8), cv2.IMREAD_GRAYSCALE)
            
            # Denoise
            img = cv2.fastNlMeansDenoising(img, h=10)
            
            # Binarize (Otsu's method)
            _, img = cv2.threshold(img, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            
            # Increase contrast
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
            img = clahe.apply(img)
            
            return cv2.imencode('.png', img)[1].tobytes()
        
        return df.withColumn("enhanced_content", enhance(col("content")))
```

---

## 3. Post-Processing Implementation

```python
class OCRPostProcessor:
    """
    Post-processing pipeline for healthcare OCR output.
    """
    
    def __init__(self, clinical_dict_path: str):
        self.clinical_dict = self._load_clinical_dictionary(clinical_dict_path)
        self.ner_model = self._load_ner_model()
    
    def postprocess(self, df: DataFrame) -> DataFrame:
        """
        Full post-processing pipeline.
        """
        return (
            df
            .transform(self.correct_spelling)
            .transform(self.expand_abbreviations)
            .transform(self.extract_entities)
            .transform(self.deidentify_phi)
            .transform(self.calculate_quality_score)
        )
    
    def expand_abbreviations(self, df: DataFrame) -> DataFrame:
        """
        Expand medical abbreviations.
        """
        ABBREVIATIONS = {
            "pt": "patient",
            "dx": "diagnosis",
            "hx": "history",
            "tx": "treatment",
            "rx": "prescription",
            "bid": "twice daily",
            "tid": "three times daily",
            "prn": "as needed",
            "htn": "hypertension",
            "dm": "diabetes mellitus",
            "chf": "congestive heart failure"
        }
        
        @udf("string")
        def expand(text: str) -> str:
            for abbr, full in ABBREVIATIONS.items():
                text = re.sub(rf'\b{abbr}\b', full, text, flags=re.IGNORECASE)
            return text
        
        return df.withColumn("normalized_text", expand(col("ocr_text")))
    
    def extract_entities(self, df: DataFrame) -> DataFrame:
        """
        Extract clinical entities (ICD codes, drugs, dates).
        """
        @udf("array<struct<type:string, value:string, confidence:float>>")
        def extract_ner(text: str):
            entities = []
            
            # ICD-10 codes (pattern: letter + 2 digits + optional decimal + digits)
            icd_pattern = r'\b[A-Z]\d{2}\.?\d{0,4}\b'
            for match in re.finditer(icd_pattern, text):
                entities.append({
                    "type": "ICD_CODE",
                    "value": match.group(),
                    "confidence": 0.95
                })
            
            # Drug names (from clinical dictionary lookup)
            # Dates
            # Lab values
            
            return entities
        
        return df.withColumn("entities", extract_ner(col("normalized_text")))
    
    def deidentify_phi(self, df: DataFrame) -> DataFrame:
        """
        De-identify PHI for safe processing.
        """
        @udf("string")
        def deidentify(text: str) -> str:
            # SSN: XXX-XX-XXXX
            text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[SSN]', text)
            
            # Phone: various formats
            text = re.sub(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', '[PHONE]', text)
            
            # MRN (medical record number)
            text = re.sub(r'\bMRN[:\s]*\d+\b', '[MRN]', text, flags=re.IGNORECASE)
            
            # Dates of birth
            # Names (using NER)
            # Addresses
            
            return text
        
        return df.withColumn("deidentified_text", deidentify(col("normalized_text")))
    
    def calculate_quality_score(self, df: DataFrame) -> DataFrame:
        """
        Calculate overall OCR quality score.
        """
        @udf("float")
        def quality_score(text: str, confidence: float) -> float:
            # Factors: OCR confidence, spell check ratio, entity extraction success
            word_count = len(text.split())
            valid_words = sum(1 for w in text.split() if self._is_valid_word(w))
            
            word_validity_ratio = valid_words / word_count if word_count > 0 else 0
            
            return (confidence * 0.5) + (word_validity_ratio * 0.5)
        
        return df.withColumn("quality_score", quality_score(col("ocr_text"), col("confidence")))
```

---

## 4. Interview Answer Framework

### **1. Pipeline Design (30 seconds)**
> "OCR pipelines have three phases: pre-processing (image enhancement, orientation correction), OCR engine (text extraction with confidence scores), and post-processing (spell correction, NER, de-identification)."

### **2. Pre-Processing Details (30 seconds)**
> "Pre-processing is critical for accuracy. I apply: denoising, binarization, deskewing, and region segmentation. For faxed documents, adaptive thresholding improves character recognition significantly."

### **3. Post-Processing Details (30 seconds)**
> "Post-processing extracts value: spell correction against medical dictionaries, abbreviation expansion (dx → diagnosis), clinical NER for ICD codes and drugs, and PHI de-identification for HIPAA compliance."

### **4. Quality Assurance (30 seconds)**
> "Every document gets a quality score (0-100) based on OCR confidence, spell-check ratio, and entity extraction success. Documents below threshold route to human review. This ensures we catch edge cases before they affect downstream analytics."

---

---

# Question 13: Architect and recommend between RAG (Retrieval-Augmented Generation) and CAG (Cache-Augmented Generation) strategies based on use-case analysis

---

## 1. RAG vs CAG Comparison

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    RAG vs CAG COMPARISON                                                 │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│                        RAG                                   CAG                         │
│         (Retrieval-Augmented Generation)           (Cache-Augmented Generation)         │
│                                                                                          │
│  ┌─────────────────────────────────┐         ┌─────────────────────────────────────┐   │
│  │         User Query              │         │         User Query                  │   │
│  └───────────────┬─────────────────┘         └───────────────┬─────────────────────┘   │
│                  │                                           │                         │
│                  ▼                                           ▼                         │
│  ┌─────────────────────────────────┐         ┌─────────────────────────────────────┐   │
│  │       Vector Search             │         │      KV Cache Lookup                │   │
│  │   (Embeddings → Top-K docs)     │         │   (Pre-computed context)            │   │
│  └───────────────┬─────────────────┘         └───────────────┬─────────────────────┘   │
│                  │                                           │                         │
│                  ▼                                           ▼                         │
│  ┌─────────────────────────────────┐         ┌─────────────────────────────────────┐   │
│  │      Augment Prompt             │         │      Prepend Cached KV              │   │
│  │   (Query + Retrieved Docs)      │         │   (No re-encoding needed)           │   │
│  └───────────────┬─────────────────┘         └───────────────┬─────────────────────┘   │
│                  │                                           │                         │
│                  ▼                                           ▼                         │
│  ┌─────────────────────────────────┐         ┌─────────────────────────────────────┐   │
│  │         LLM Generation          │         │         LLM Generation              │   │
│  └─────────────────────────────────┘         └─────────────────────────────────────┘   │
│                                                                                          │
│  KEY DIFFERENCE:                                                                         │
│  • RAG: Retrieves documents at query time, encodes them in the prompt                   │
│  • CAG: Pre-computes KV cache for fixed context, reuses across queries                  │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. When to Use RAG vs CAG

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    RAG vs CAG DECISION MATRIX                                            │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  Factor                    │ RAG                    │ CAG                               │
│  ──────────────────────────┼────────────────────────┼───────────────────────────────────│
│                            │                        │                                   │
│  Knowledge Base Size       │ Large (millions docs)  │ Small-Medium (<100K tokens)       │
│                            │                        │                                   │
│  Update Frequency          │ Frequent updates       │ Stable/infrequent updates         │
│                            │                        │                                   │
│  Query Latency Needs       │ Can tolerate ~500ms    │ Needs <100ms                      │
│                            │                        │                                   │
│  Context Relevance         │ Query-dependent        │ Entire corpus always relevant     │
│                            │                        │                                   │
│  Cost Sensitivity          │ Per-query retrieval    │ One-time cache generation         │
│                            │                        │                                   │
│  Infrastructure            │ Vector DB required     │ KV cache storage required         │
│                            │                        │                                   │
│  ──────────────────────────┼────────────────────────┼───────────────────────────────────│
│                            │                        │                                   │
│  Best For                  │ • Large document       │ • Fixed reference data            │
│                            │   collections          │ • High-frequency queries          │
│                            │ • Dynamic knowledge    │ • Latency-critical apps           │
│                            │ • Diverse queries      │ • Consistent context needs        │
│                            │                        │                                   │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Population Advyzer Use Cases

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    USE CASE MAPPING                                                      │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  Use Case                               │ Recommendation │ Reasoning                     │
│  ───────────────────────────────────────┼────────────────┼───────────────────────────────│
│                                         │                │                               │
│  CMS HCC Model Reference (V24, V28)     │ CAG            │ Fixed reference, high-freq    │
│                                         │                │ queries, always relevant      │
│                                         │                │                               │
│  ICD-10 Code Lookups                    │ CAG            │ Stable codeset, latency needs │
│                                         │                │                               │
│  Clinical Guidelines (CMS/HEDIS)        │ Hybrid         │ Semi-stable, query-specific   │
│                                         │                │ relevance                     │
│                                         │                │                               │
│  Member Chart Notes                     │ RAG            │ Large corpus, member-specific │
│                                         │                │ retrieval needed              │
│                                         │                │                               │
│  Historical Gap Closure Patterns        │ RAG            │ Large history, query-specific │
│                                         │                │                               │
│  Provider Documentation                 │ RAG            │ Millions of documents         │
│                                         │                │                               │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Implementation Architecture

```python
# Hybrid RAG + CAG Architecture

class HybridKnowledgeSystem:
    """
    Combines CAG for stable reference data with RAG for dynamic knowledge.
    """
    
    def __init__(self):
        # CAG: Pre-cached reference data
        self.cag_cache = KVCache()
        self.cag_cache.add("cms_hcc_v28", self._encode_hcc_model("V28"))
        self.cag_cache.add("icd10_codes", self._encode_icd10())
        
        # RAG: Vector store for dynamic documents
        self.vector_store = VectorStore()
        self.embeddings = EmbeddingModel()
    
    async def query(self, question: str, member_bid: str = None) -> str:
        """
        Route to appropriate knowledge source.
        """
        # Determine query type
        query_type = self._classify_query(question)
        
        if query_type == "reference_lookup":
            # CAG: Use pre-cached context
            return await self._cag_query(question)
        
        elif query_type == "member_specific":
            # RAG: Retrieve member-specific documents
            return await self._rag_query(question, member_bid)
        
        else:
            # Hybrid: Combine both
            return await self._hybrid_query(question, member_bid)
    
    async def _cag_query(self, question: str) -> str:
        """
        Cache-Augmented Generation for reference data.
        """
        # Get pre-computed KV cache
        kv_cache = self.cag_cache.get("cms_hcc_v28")
        
        # Generate with cached context (no retrieval needed)
        response = await self.llm.generate(
            question,
            kv_cache=kv_cache  # Pre-computed attention cache
        )
        
        return response
    
    async def _rag_query(self, question: str, member_bid: str) -> str:
        """
        Retrieval-Augmented Generation for dynamic knowledge.
        """
        # Embed query
        query_embedding = self.embeddings.encode(question)
        
        # Retrieve relevant documents
        docs = self.vector_store.search(
            query_embedding,
            filter={"member_bid": member_bid},
            top_k=5
        )
        
        # Augment prompt
        context = "\n\n".join([d.content for d in docs])
        augmented_prompt = f"""
        Context:
        {context}
        
        Question: {question}
        """
        
        response = await self.llm.generate(augmented_prompt)
        
        return response
    
    async def _hybrid_query(self, question: str, member_bid: str) -> str:
        """
        Combine CAG (reference) + RAG (member-specific).
        """
        # CAG: Get reference context from cache
        reference_kv = self.cag_cache.get("cms_hcc_v28")
        
        # RAG: Retrieve member-specific docs
        query_embedding = self.embeddings.encode(question)
        member_docs = self.vector_store.search(
            query_embedding,
            filter={"member_bid": member_bid},
            top_k=3
        )
        
        # Combine: Cached reference + retrieved member docs
        augmented_prompt = f"""
        Member Context:
        {"\n".join([d.content for d in member_docs])}
        
        Question: {question}
        """
        
        response = await self.llm.generate(
            augmented_prompt,
            kv_cache=reference_kv  # Reference always available via cache
        )
        
        return response
```

---

## 5. Interview Answer Framework

### **1. Define (30 seconds)**
> "RAG retrieves relevant documents at query time and adds them to the prompt. CAG pre-computes the attention KV-cache for stable context and reuses it across queries - no retrieval or re-encoding needed."

### **2. Decision Criteria (1 minute)**
> "Use **RAG** when:
> - Knowledge base is large (millions of documents)
> - Context is query-specific (retrieve only relevant docs)
> - Content updates frequently
>
> Use **CAG** when:
> - Reference data is stable and always relevant
> - Latency is critical (<100ms)
> - Same context applies to many queries"

### **3. Healthcare Example (30 seconds)**
> "For Population Advyzer, I'd use **CAG** for CMS HCC model reference (stable, high-frequency lookups) and **RAG** for member chart notes (large corpus, member-specific retrieval). A hybrid architecture gives us the best of both: fast reference lookups via cache plus flexible document retrieval."

---

---

# End-to-End Architecture: Databricks → Snowflake → Tableau

---

## Complete Data Flow

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    POPULATION ADVYZER END-TO-END ARCHITECTURE                            │
└──────────────────────────────────────────────────────────────────────────────────────────┘

  DATA SOURCES                    PROCESSING (Databricks)                 CONSUMPTION
  ────────────                    ───────────────────────                 ───────────

  ┌───────────────┐
  │ Claims (837)  │ ─┐
  └───────────────┘  │
  ┌───────────────┐  │         ┌─────────────────────────────────────────────────────────┐
  │ Eligibility   │ ─┼────────▶│                    DATABRICKS                           │
  └───────────────┘  │         │                                                         │
  ┌───────────────┐  │         │  ┌─────────┐   ┌─────────┐   ┌─────────┐              │
  │ Provider      │ ─┤         │  │ BRONZE  │ → │ SILVER  │ → │  GOLD   │              │
  └───────────────┘  │         │  │         │   │         │   │         │              │
  ┌───────────────┐  │         │  │ Raw     │   │ Cleaned │   │ Curated │              │
  │ MOR Files     │ ─┘         │  │ Ingest  │   │ Transform│  │ Scored  │              │
  └───────────────┘            │  └─────────┘   └─────────┘   └────┬────┘              │
                               │                                    │                   │
                               │  Unity Catalog: pop_stg            │                   │
                               │                                    │                   │
                               │  Key Tables:                       │                   │
                               │  • member_demographics             │                   │
                               │  • hcc_scores                      │                   │
                               │  • gap_suspecting                  │                   │
                               └────────────────────────────────────┼───────────────────┘
                                                                    │
                                                                    │ Delta Sharing
                                                                    │ (Iceberg Format)
                                                                    ▼
                               ┌─────────────────────────────────────────────────────────┐
                               │                    SNOWFLAKE                            │
                               │                                                         │
                               │  External Tables (Iceberg):                             │
                               │  • pop_stg.uatplan1_ma_dashboard.member_level          │
                               │  • pop_stg.uatplan1_ma_dashboard.member_hcc_level      │
                               │                                                         │
                               │  Aggregation Views:                                     │
                               │  • vw_raf_summary                                       │
                               │  • vw_gap_closure_trends                                │
                               │  • vw_provider_performance                              │
                               │                                                         │
                               └────────────────────────────────────┬────────────────────┘
                                                                    │
                                                                    │ Snowflake Connector
                                                                    ▼
                               ┌─────────────────────────────────────────────────────────┐
                               │                     TABLEAU                             │
                               │                                                         │
                               │  ┌───────────────────┐   ┌───────────────────┐         │
                               │  │  RAF Dashboard    │   │  Gap Closure      │         │
                               │  │                   │   │  Dashboard        │         │
                               │  │ • Member scores   │   │ • Open gaps       │         │
                               │  │ • Trend analysis  │   │ • Provider rates  │         │
                               │  │ • Model comparison│   │ • ROI projections │         │
                               │  └───────────────────┘   └───────────────────┘         │
                               │                                                         │
                               │  ┌───────────────────┐   ┌───────────────────┐         │
                               │  │  Provider         │   │  Executive        │         │
                               │  │  Scorecard        │   │  Summary          │         │
                               │  └───────────────────┘   └───────────────────┘         │
                               │                                                         │
                               └─────────────────────────────────────────────────────────┘
```

---

## Key Output Tables for Tableau

```sql
-- Member Level Summary (Tableau Primary Source)
SELECT * FROM pop_stg.uatplan1_ma_dashboard.member_level
LIMIT 5;

/*
MEMBER_BID | RISK_YEAR | RAF_SCORE | HCC_COUNT | GAP_COUNT | CLOSURE_RATE
----------------------------------------------------------------------
123456     | 2026      | 1.245     | 5         | 2         | 0.75
234567     | 2026      | 0.892     | 3         | 1         | 0.80
*/

-- Member HCC Detail (Drilldown Source)
SELECT * FROM pop_stg.uatplan1_ma_dashboard.member_hcc_level
LIMIT 5;

/*
MEMBER_BID | HCC_CODE | HCC_DESCRIPTION | SOURCE | STATUS | COEFFICIENT
------------------------------------------------------------------------
123456     | HCC19    | Diabetes w/comp | Claim  | Closed | 0.302
123456     | HCC85    | CHF             | Gap    | Open   | 0.323
*/
```

---

---

# Appendix B: Population Advyzer System Architecture

---

## B.1 High-Level System Overview

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                    POPULATION ADVYZER - SYSTEM ARCHITECTURE                              │
│                    Medicare Advantage Risk Adjustment Platform                           │
└──────────────────────────────────────────────────────────────────────────────────────────┘

                                 ┌─────────────────────┐
                                 │    DATA SOURCES     │
                                 └──────────┬──────────┘
                                            │
        ┌───────────────────────────────────┼───────────────────────────────────┐
        │                                   │                                   │
        ▼                                   ▼                                   ▼
┌───────────────┐               ┌───────────────┐               ┌───────────────┐
│   CLAIMS      │               │  ELIGIBILITY  │               │   MOR FILES   │
│   (837 P/I)   │               │    (834)      │               │ (CMS Monthly) │
├───────────────┤               ├───────────────┤               ├───────────────┤
│ • Professional│               │ • Member info │               │ • Prior HCCs  │
│ • Institutional               │ • Coverage    │               │ • RAF scores  │
│ • Diagnosis   │               │ • Enrollment  │               │ • Payment adj │
│ • Procedures  │               │ • Demographics│               │ • ESRD status │
└───────┬───────┘               └───────┬───────┘               └───────┬───────┘
        │                               │                               │
        └───────────────────────────────┼───────────────────────────────┘
                                        │
                                        ▼
        ┌───────────────────────────────────────────────────────────────────────┐
        │                         DATABRICKS PLATFORM                           │
        │                         (Unity Catalog)                               │
        │                                                                       │
        │  ┌─────────────────────────────────────────────────────────────────┐ │
        │  │                      DATA INGESTION                              │ │
        │  │  src/spark/data_loader/                                          │ │
        │  │  • data_loader_util.py (Bronze layer ingestion)                  │ │
        │  │  • Schema validation, deduplication                              │ │
        │  └─────────────────────────────────────────────────────────────────┘ │
        │                               │                                       │
        │                               ▼                                       │
        │  ┌─────────────────────────────────────────────────────────────────┐ │
        │  │                   DATA STANDARDIZATION                           │ │
        │  │  src/spark/data_standardization/                                 │ │
        │  │  • transformations_commons.py (Silver layer)                     │ │
        │  │  • Standard column names, data types                             │ │
        │  └─────────────────────────────────────────────────────────────────┘ │
        │                               │                                       │
        │              ┌────────────────┴────────────────┐                     │
        │              │                                 │                     │
        │              ▼                                 ▼                     │
        │  ┌───────────────────────┐         ┌───────────────────────┐        │
        │  │     RISK ENGINE      │         │    GAP SUSPECTING     │        │
        │  │    (CMS Scoring)     │         │    (Gap Closure)      │        │
        │  │                      │         │                       │        │
        │  │ src/spark/cms/       │         │ src/spark/gp_suspecting/       │
        │  └───────────┬──────────┘         └───────────┬───────────┘        │
        │              │                                 │                     │
        │              └────────────────┬────────────────┘                     │
        │                               │                                       │
        │                               ▼                                       │
        │  ┌─────────────────────────────────────────────────────────────────┐ │
        │  │                      GOLD LAYER                                  │ │
        │  │  pop_stg.{plan}_ma_dashboard.member_level                       │ │
        │  │  pop_stg.{plan}_ma_dashboard.member_hcc_level                   │ │
        │  └─────────────────────────────────────────────────────────────────┘ │
        │                                                                       │
        └───────────────────────────────┬───────────────────────────────────────┘
                                        │
                                        │ Delta Sharing (Iceberg)
                                        ▼
        ┌───────────────────────────────────────────────────────────────────────┐
        │                          SNOWFLAKE                                    │
        │                    (Analytics & Reporting)                            │
        └───────────────────────────────┬───────────────────────────────────────┘
                                        │
                                        ▼
        ┌───────────────────────────────────────────────────────────────────────┐
        │                           TABLEAU                                     │
        │                      (Business Dashboards)                            │
        └───────────────────────────────────────────────────────────────────────┘
```

---

## B.2 CMS Risk Engine - Detailed Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         CMS RISK ENGINE ARCHITECTURE                                     │
│                    src/spark/cms/ - Risk Adjustment Scoring                              │
└──────────────────────────────────────────────────────────────────────────────────────────┘

                        ┌─────────────────────────────┐
                        │       INPUT DATA            │
                        │                             │
                        │ • Member Demographics       │
                        │ • Claims (Dx codes)         │
                        │ • Eligibility               │
                        │ • MOR Prior Year Data       │
                        └──────────────┬──────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         MA MODEL INPUT PREPARATION                                       │
│                         ma_model_input_data.py                                          │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐ │
│  │  Member         │   │  Diagnosis      │   │  Qualifying     │   │  MOR Health     │ │
│  │  Demographics   │   │  Collection     │   │  Claims Filter  │   │  Events         │ │
│  │                 │   │                 │   │                 │   │                 │ │
│  │ • Age/Gender    │   │ • ICD-10 codes  │   │ • Claim dates   │   │ • Prior HCCs    │ │
│  │ • OREC/CREC     │   │ • Service dates │   │ • POS/TOB       │   │ • ESRD status   │ │
│  │ • Dual status   │   │ • Providers     │   │ • Claim status  │   │ • Hospice       │ │
│  └────────┬────────┘   └────────┬────────┘   └────────┬────────┘   └────────┬────────┘ │
│           │                     │                     │                     │           │
│           └─────────────────────┴─────────────────────┴─────────────────────┘           │
│                                         │                                               │
└─────────────────────────────────────────┼───────────────────────────────────────────────┘
                                          │
                                          ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                            ICD → HCC MAPPING                                             │
│                            icd_hcc_mapping.csv                                           │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   ICD-10 Code    │  HCC (V24)  │  HCC (V28)  │  Description                             │
│   ───────────────┼─────────────┼─────────────┼──────────────────────────────────────────│
│   E11.65         │  HCC18      │  HCC37      │  Type 2 diabetes with hyperglycemia      │
│   I50.9          │  HCC85      │  HCC226     │  Heart failure, unspecified              │
│   J44.1          │  HCC111     │  HCC280     │  COPD with acute exacerbation            │
│   N18.4          │  HCC137     │  HCC329     │  Chronic kidney disease, stage 4         │
│                                                                                          │
└──────────────────────────────────────────┬───────────────────────────────────────────────┘
                                           │
                  ┌────────────────────────┬┴───────────────────────┐
                  │                        │                        │
                  ▼                        ▼                        ▼
┌─────────────────────────┐  ┌─────────────────────────┐  ┌─────────────────────────┐
│      CMS-HCC MODEL      │  │     CMS-RxHCC MODEL     │  │      ESRD MODEL         │
│      cms_hcc_main.py    │  │     cms_rxhcc_main.py   │  │      cms_esrd_main.py   │
├─────────────────────────┤  ├─────────────────────────┤  ├─────────────────────────┤
│                         │  │                         │  │                         │
│ • V24 Model (2020-2024) │  │ • Rx Hierarchies        │  │ • Dialysis status       │
│ • V28 Model (2024+)     │  │ • Drug categories       │  │ • Transplant status     │
│ • Blending (transition) │  │ • Pharmacy claims       │  │ • Functioning graft     │
│                         │  │                         │  │                         │
│ Key Files:              │  │ Key Files:              │  │ Key Files:              │
│ • cms_hcc_helper.py     │  │ • cms_rxhcc_helper.py   │  │ • cms_esrd_helper.py    │
│ • hcc_hierarchy.py      │  │ • rxhcc_hierarchy.py    │  │ • esrd_coefficients.py  │
│                         │  │                         │  │                         │
└───────────┬─────────────┘  └───────────┬─────────────┘  └───────────┬─────────────┘
            │                            │                            │
            └────────────────────────────┼────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         HIERARCHY & INTERACTION PROCESSING                               │
│                         hcc_hierarchy.py, disease_interaction.py                         │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  HIERARCHY RULES (Example):                      DISEASE INTERACTIONS:                   │
│  ─────────────────────────────                   ─────────────────────                   │
│  HCC17 → supersedes → HCC18, HCC19               HCC47 + HCC85 = INT5 (bonus)           │
│  HCC8  → supersedes → HCC9, HCC10, HCC11         HCC96 + HCC85 = INT6 (bonus)           │
│  HCC85 → supersedes → HCC86, HCC87, HCC88        HCC8  + HCC18 = INT9 (bonus)           │
│                                                                                          │
│  Only the HIGHEST severity HCC in a             Additional coefficients for             │
│  hierarchy group is counted for payment         specific HCC combinations               │
│                                                                                          │
└──────────────────────────────────────────┬───────────────────────────────────────────────┘
                                           │
                                           ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         RAF SCORE CALCULATION                                            │
│                         cms_persistence_hcc.py, member_persistent_hcc.py                 │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   RAF_SCORE = Demographic_Factor + Σ(HCC_Coefficient) + Σ(Interaction_Coefficient)      │
│                                                                                          │
│   Example Calculation:                                                                   │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │  Member: 72-year-old Male, Community, Non-Dual                                  │   │
│   │                                                                                  │   │
│   │  Demographic Factor (M70_74):           0.450                                   │   │
│   │  HCC19 (Diabetes with complications):   0.302                                   │   │
│   │  HCC85 (CHF):                           0.323                                   │   │
│   │  HCC96 (Specified Heart Arrhythmias):   0.265                                   │   │
│   │  INT6 (HCC85 + HCC96 interaction):      0.156                                   │   │
│   │  ─────────────────────────────────────────────                                  │   │
│   │  TOTAL RAF SCORE:                       1.496                                   │   │
│   │                                                                                  │   │
│   │  Expected Annual Cost = RAF × Base Rate = 1.496 × $12,000 = $17,952            │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## B.3 Gap Suspecting Engine - Detailed Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                      GAP SUSPECTING ENGINE ARCHITECTURE                                  │
│                      src/spark/gp_suspecting/ - HCC Gap Closure                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘

                        ┌─────────────────────────────┐
                        │       INPUT DATA            │
                        │                             │
                        │ • Current Year Claims       │
                        │ • Prior Year HCCs (MOR)     │
                        │ • Member Demographics       │
                        │ • Reference Tables          │
                        └──────────────┬──────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         GAP SUSPECTING MAIN ORCHESTRATION                                │
│                         gap_suspecting_main.py                                           │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  def run_gap_suspecting(plan_name, risk_year, model_version):                           │
│      """                                                                                 │
│      Main entry point for gap suspecting pipeline.                                      │
│      """                                                                                 │
│      # 1. Load member population                                                         │
│      members = load_member_population(plan_name, risk_year)                             │
│                                                                                          │
│      # 2. Get current year confirmed HCCs                                                │
│      current_hccs = get_current_year_hccs(members, risk_year)                           │
│                                                                                          │
│      # 3. Get prior year HCCs from MOR                                                   │
│      prior_hccs = get_prior_year_hccs(members, risk_year - 1)                           │
│                                                                                          │
│      # 4. Identify gaps (prior year HCCs not yet captured)                               │
│      gaps = identify_hcc_gaps(prior_hccs, current_hccs)                                  │
│                                                                                          │
│      # 5. Apply suspecting methods                                                       │
│      suspected_gaps = apply_suspecting_methods(gaps)                                     │
│                                                                                          │
│      # 6. Calculate confidence scores                                                    │
│      scored_gaps = calculate_confidence_scores(suspected_gaps)                          │
│                                                                                          │
│      # 7. Apply suppressions                                                             │
│      final_gaps = apply_suppressions(scored_gaps)                                        │
│                                                                                          │
│      return final_gaps                                                                   │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         GAP IDENTIFICATION LOGIC                                         │
│                         gap_suspecting_helper.py                                         │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   GAP = Prior Year HCC that is NOT YET captured in Current Year                         │
│                                                                                          │
│   ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│   │                                                                                    │ │
│   │  Prior Year (2025)              Current Year (2026)           Gap Status          │ │
│   │  ─────────────────              ──────────────────           ──────────           │ │
│   │                                                                                    │ │
│   │  HCC19 (Diabetes)     ───────▶  HCC19 (Claim found)    =    CLOSED ✓             │ │
│   │  HCC85 (CHF)          ───────▶  No claim found         =    OPEN GAP ⚠️          │ │
│   │  HCC96 (Arrhythmia)   ───────▶  HCC96 (Claim found)    =    CLOSED ✓             │ │
│   │                                                                                    │ │
│   └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         SUSPECTING METHODS                                               │
│                         (Confidence Score Calculation)                                   │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  METHOD 1: CHRONIC CONDITION PERSISTENCE                                                │
│  ─────────────────────────────────────────                                              │
│  • Chronic conditions (diabetes, CHF, COPD) persist year-over-year                      │
│  • High confidence if condition appeared in multiple prior years                        │
│  • Reference: ref_chronic_condition.csv                                                 │
│                                                                                          │
│  METHOD 2: RELATED CLAIMS EVIDENCE                                                      │
│  ─────────────────────────────────────                                                  │
│  • Look for related claims that suggest condition still exists                          │
│  • Example: Insulin prescription suggests diabetes still present                        │
│  • Example: CHF medication suggests heart failure ongoing                               │
│                                                                                          │
│  METHOD 3: PROCEDURE/LAB EVIDENCE                                                       │
│  ─────────────────────────────────                                                      │
│  • Lab tests or procedures associated with the condition                                │
│  • Example: HbA1c test suggests diabetes monitoring                                     │
│  • Example: Echocardiogram suggests cardiac condition                                   │
│                                                                                          │
│  METHOD 4: PROVIDER SPECIALTY VISITS                                                    │
│  ───────────────────────────────────                                                    │
│  • Visits to specialists treating the condition                                         │
│  • Example: Endocrinologist visit for diabetes patient                                  │
│  • Example: Cardiologist visit for CHF patient                                          │
│                                                                                          │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │  CONFIDENCE SCORE CALCULATION                                                      │ │
│  │                                                                                     │ │
│  │  Score = (Method1_Weight × Evidence1) + (Method2_Weight × Evidence2) + ...        │ │
│  │                                                                                     │ │
│  │  Weights from: ref_method_prior_year.csv                                           │ │
│  │                                                                                     │ │
│  │  Final Score Ranges:                                                               │ │
│  │  • HIGH (0.8 - 1.0):   Strong evidence, high priority for outreach                │ │
│  │  • MEDIUM (0.5 - 0.8): Moderate evidence, worth investigating                     │ │
│  │  • LOW (0.0 - 0.5):    Weak evidence, lower priority                              │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         SUPPRESSION RULES                                                │
│                         (Filter Out Invalid Gaps)                                        │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  SUPPRESSION 1: HIERARCHY SUPPRESSION                                                   │
│  ────────────────────────────────────                                                   │
│  • If higher HCC in same hierarchy is present, suppress lower gap                       │
│  • Example: If HCC17 captured, suppress HCC18 gap (diabetes hierarchy)                  │
│                                                                                          │
│  SUPPRESSION 2: DEATH/DISENROLLMENT                                                     │
│  ────────────────────────────────────                                                   │
│  • Suppress gaps for members who died or disenrolled                                    │
│  • No point chasing gaps for members no longer in plan                                  │
│                                                                                          │
│  SUPPRESSION 3: CONDITION-SPECIFIC RULES                                                │
│  ────────────────────────────────────────                                               │
│  • Some conditions can resolve (acute conditions)                                       │
│  • Example: Pregnancy-related HCCs after delivery                                       │
│  • Example: Trauma HCCs after healing period                                            │
│                                                                                          │
│  SUPPRESSION 4: ADMINISTRATIVE EXCLUSIONS                                               │
│  ────────────────────────────────────────                                               │
│  • Plan-specific exclusions                                                             │
│  • Regulatory compliance requirements                                                   │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         OUTPUT: SUSPECTED GAPS                                           │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐│
│  │ MEMBER_BID │ HCC  │ DESCRIPTION        │ CONF_SCORE │ METHODS_MATCHED │ RAF_IMPACT ││
│  │────────────┼──────┼────────────────────┼────────────┼─────────────────┼────────────││
│  │ 123456     │ HCC85│ CHF                │ 0.92       │ 1,2,4           │ $3,876     ││
│  │ 123456     │ HCC111│COPD w/ exacerb    │ 0.78       │ 1,3             │ $2,544     ││
│  │ 234567     │ HCC18│ Diabetes w/o comp  │ 0.65       │ 2               │ $1,284     ││
│  └─────────────────────────────────────────────────────────────────────────────────────┘│
│                                                                                          │
│  Downstream Usage:                                                                       │
│  • Provider outreach lists                                                               │
│  • Member engagement campaigns                                                           │
│  • Care management prioritization                                                        │
│  • RAF revenue forecasting                                                               │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## B.4 Key Reference Tables

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         REFERENCE DATA (src/sql/data/)                                   │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  icd_hcc_mapping.csv          │ ICD-10 to HCC mappings for V24/V28 models               │
│  ref_chronic_condition.csv    │ Chronic vs acute condition classification               │
│  ref_method_prior_year.csv    │ Suspecting method weights and thresholds                │
│  ref_place_of_service_tob.csv │ Valid POS/TOB codes for qualifying claims               │
│  ref_risk_csr.csv             │ CSR (Cost Share Reduction) factors                      │
│  ref_risk_metal.csv           │ Metal tier risk adjustments                             │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## B.5 Data Flow Summary

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         POPULATION ADVYZER DATA FLOW                                     │
└──────────────────────────────────────────────────────────────────────────────────────────┘

  BRONZE LAYER                 SILVER LAYER                 GOLD LAYER
  (Raw Ingestion)              (Standardized)               (Business Ready)
  ─────────────                ──────────────               ────────────────

  ┌───────────────┐           ┌───────────────┐           ┌───────────────┐
  │ raw_claims    │ ────────▶ │ stg_claims    │ ────────▶ │ hcc_scores    │
  └───────────────┘           └───────────────┘           └───────────────┘
  ┌───────────────┐           ┌───────────────┐           ┌───────────────┐
  │ raw_member    │ ────────▶ │ stg_member    │ ────────▶ │ gap_suspecting│
  └───────────────┘           └───────────────┘           └───────────────┘
  ┌───────────────┐           ┌───────────────┐           ┌───────────────┐
  │ raw_mor       │ ────────▶ │ stg_mor       │ ────────▶ │ member_level  │
  └───────────────┘           └───────────────┘           └───────────────┘

  Catalog: pop_{env}.{plan}_ingestion → pop_{env}.{plan}_transformation → pop_{env}.{plan}_curation
```

---

*Document created for AI Solution Architect interview preparation*
*Based on Population Advyzer codebase analysis*
*Last updated: 2026-06-17*
