# Claude Code Skills and Agents Setup Guide

**Version:** 1.0  
**Author:** Mukesh Kumar  
**Date:** 2026-05-04  
**Purpose:** Step-by-step guide for setting up custom Skills and Agents in Claude Code

---

## Table of Contents

1. [Overview](#1-overview)
2. [Prerequisites](#2-prerequisites)
3. [Directory Structure](#3-directory-structure)
4. [Setting Up Skills](#4-setting-up-skills)
5. [Setting Up Agents](#5-setting-up-agents)
6. [CLAUDE.md Configuration](#6-claudemd-configuration)
7. [Available Skills Reference](#7-available-skills-reference)
8. [Available Agents Reference](#8-available-agents-reference)
9. [Testing Your Setup](#9-testing-your-setup)
10. [Best Practices](#10-best-practices)

---

## 1. Overview

Claude Code supports two types of customizations:

| Type | Purpose | Location | Invocation |
|------|---------|----------|------------|
| **Skills** | Slash commands for specialized workflows | `.github/skills/` or `.claude/commands/` | `/skill-name <args>` |
| **Agents** | Background workers for complex tasks | `.github/agents/` | Automatically used by Claude |

### Current Project Setup Summary

| Component | Count | Description |
|-----------|-------|-------------|
| **Skills** | 6 | spec, test, bug, reconcile, design, plan |
| **Agents** | 5 | spec-analyst, feature-dev, test-runner, reconciler, doc-generator |
| **Special Skill** | 1 | gap-suspect (with reference docs) |

---

## 2. Prerequisites

1. **Claude Code CLI** installed and authenticated
2. **Git repository** initialized
3. **VS Code** or terminal access
4. Basic understanding of Markdown and YAML frontmatter

### Installation Check
```bash
# Verify Claude Code is installed
claude --version

# Verify you're in a git repository
git status
```

---

## 3. Directory Structure

Create the following folder structure in your project root:

```
your-project/
├── .github/
│   ├── agents/                    # Agent definitions
│   │   ├── spec-analyst.md
│   │   ├── feature-dev.md
│   │   ├── test-runner.md
│   │   ├── reconciler.md
│   │   └── doc-generator.md
│   └── skills/                    # Skill definitions
│       ├── spec.md
│       ├── test.md
│       ├── bug.md
│       ├── reconcile.md
│       ├── design.md
│       ├── plan.md
│       └── gap-suspect/           # Complex skill with references
│           ├── SKILL.md
│           └── references/
│               ├── methods.md
│               ├── pipeline-flow.md
│               └── risk-scoring.md
├── .claude/
│   └── commands/                  # Alternative skill location
│       ├── spec.md
│       ├── bug.md
│       └── ...
├── CLAUDE.md                      # Project-level instructions
└── .gitignore                     # Add .claude/ and .github/agents/ if needed
```

### Create Directories (Bash)
```bash
mkdir -p .github/agents
mkdir -p .github/skills
mkdir -p .claude/commands
```

---

## 4. Setting Up Skills

Skills are invoked using slash commands (e.g., `/spec`, `/bug`).

### Step 4.1: Create a Skill File

Create a new `.md` file in `.github/skills/` or `.claude/commands/`:

```bash
# Create skill file
touch .github/skills/my-skill.md
```

### Step 4.2: Add Frontmatter

Every skill file MUST start with YAML frontmatter:

```markdown
---
name: skill-name
description: "Brief description of what the skill does and when to use it"
argument-hint: "What argument the user should provide (optional)"
---
```

### Step 4.3: Write Skill Instructions

After the frontmatter, add detailed instructions:

```markdown
---
name: my-skill
description: "My custom skill for doing X"
argument-hint: "Describe what you want"
---

# My Skill Name

## Purpose
[What this skill does]

## Workflow
### Phase 1: [Name]
1. Step 1
2. Step 2

### Phase 2: [Name]
1. Step 1
2. Step 2

## Output Format
[Expected output structure]

## Example
**User:** `/my-skill do something`
**Claude:** [Expected response]
```

### Step 4.4: Skill Template (Copy-Paste Ready)

```markdown
---
name: example-skill
description: "Example skill template - replace with your description"
argument-hint: "Describe the task"
---

# Example Skill

## Purpose
Transform user requests into actionable outcomes through a structured workflow.

## When to Use
- Use case 1
- Use case 2

## Workflow

### Phase 1: Analysis
1. Parse the user request
2. Identify requirements
3. Ask clarifying questions if needed

### Phase 2: Execution
1. Perform the task
2. Validate results
3. Report completion

## Output Format

```markdown
## Result

### Summary
[Brief summary]

### Details
[Detailed output]

### Next Steps
[Recommended follow-up actions]
```

## Example Interaction

**User:** `/example-skill analyze the login feature`

**Claude:**
> Analyzing the login feature...
> [Results]
```

---

## 5. Setting Up Agents

Agents are background workers that Claude uses automatically for complex tasks.

### Step 5.1: Create an Agent File

```bash
touch .github/agents/my-agent.md
```

### Step 5.2: Add Agent Frontmatter

Agent frontmatter includes tool permissions:

```markdown
---
name: agent-name
description: "Description of what this agent does and when Claude should use it"
model: sonnet                    # or opus, haiku
tools:
  - Read
  - Write
  - Edit
  - Glob
  - Grep
  - Bash
  - WebFetch
---
```

### Step 5.3: Available Tools

| Tool | Purpose |
|------|---------|
| `Read` | Read files from filesystem |
| `Write` | Create new files |
| `Edit` | Modify existing files |
| `Glob` | Find files by pattern |
| `Grep` | Search file contents |
| `Bash` | Execute shell commands |
| `WebFetch` | Fetch web content |
| `WebSearch` | Search the web |

### Step 5.4: Agent Template (Copy-Paste Ready)

```markdown
---
name: my-agent
description: "My custom agent for handling X tasks. Use when user requests Y or needs Z."
model: sonnet
tools:
  - Read
  - Write
  - Edit
  - Glob
  - Grep
  - Bash
---

# My Agent Name

You are a [Role] specializing in [Domain]. Your responsibilities include:

1. **Responsibility 1** - Description
2. **Responsibility 2** - Description
3. **Responsibility 3** - Description

## Context

[Project-specific context the agent needs to know]

## Your Workflow

### Step 1: [Name]
- Action 1
- Action 2

### Step 2: [Name]
- Action 1
- Action 2

## Patterns to Follow

### Code Pattern
```python
# Example code pattern
def example():
    pass
```

### Naming Conventions
| Element | Convention | Example |
|---------|------------|---------|
| Variables | snake_case | my_variable |
| Classes | PascalCase | MyClass |

## Do NOT

- Don't do X
- Don't do Y
- Don't do Z

## Output Format

When reporting results, use this format:
```markdown
## Task Complete

### Summary
[Brief summary]

### Changes Made
- Change 1
- Change 2

### Validation
- [x] Validation 1
- [x] Validation 2
```
```

---

## 6. CLAUDE.md Configuration

The `CLAUDE.md` file in your project root provides global instructions to Claude.

### Step 6.1: Create CLAUDE.md

```bash
touch CLAUDE.md
```

### Step 6.2: Basic Structure

```markdown
# CLAUDE.md - Project Context for Claude Code

## User Context

**Role:** [Your Role]
**Name:** [Your Name]

### Primary Responsibilities
- Responsibility 1
- Responsibility 2

### Workflow Preferences
- Preference 1
- Preference 2

---

## Project Overview
[Brief project description]

- **Stack:** [Technologies used]
- **Storage:** [Data storage details]

---

## Available Skills

| Skill | Usage | Description |
|-------|-------|-------------|
| `/spec` | `/spec <description>` | Spec-driven development |
| `/test` | `/test <feature>` | Generate and run tests |
| `/bug` | `/bug <description>` | Investigate and fix bugs |

---

## Directory Structure
```
src/
├── module1/
├── module2/
```

---

## Code Conventions

### Naming
- Files: snake_case.py
- Functions: snake_case()
- Classes: PascalCase

### Patterns
[Code patterns to follow]

---

## Environment Connections

[Database connections, API endpoints, etc.]

---

## Common Commands

```bash
# Run tests
pytest test/ -v

# Format code
black src/
```
```

---

## 7. Available Skills Reference

### Skill 1: `/spec` - Spec-Driven Development

**Purpose:** Transform feature specifications into working code

**Usage:** `/spec <feature description>`

**Workflow:**
1. Clarify requirements (ask questions)
2. Create implementation plan
3. Get user approval
4. Implement autonomously
5. Report completion

**File Location:** `.github/skills/spec.md`

---

### Skill 2: `/test` - Testing

**Purpose:** Generate and run comprehensive unit tests

**Usage:** `/test <feature or file>`

**Workflow:**
1. Analyze code to test
2. Design test cases (positive, negative, edge cases)
3. Write pytest tests
4. Execute tests
5. Report results with coverage

**File Location:** `.github/skills/test.md`

---

### Skill 3: `/bug` - Bug Investigation

**Purpose:** Investigate and fix bugs systematically

**Usage:** `/bug <description or JIRA ticket>`

**Workflow:**
1. Understand bug (symptoms, expected behavior)
2. Investigate (code analysis, data analysis)
3. Identify root cause
4. Propose fix with impact analysis
5. Implement after approval
6. Add regression test

**File Location:** `.github/skills/bug.md`

---

### Skill 4: `/reconcile` - Data Reconciliation

**Purpose:** Compare data between Databricks and SQL Server

**Usage:** `/reconcile <table> <environment>`

**Workflow:**
1. Connect to both data sources
2. Execute comparison queries
3. Identify discrepancies
4. Generate variance report
5. Provide analysis

**File Location:** `.github/skills/reconcile.md`

---

### Skill 5: `/design` - Documentation

**Purpose:** Generate technical design documents and diagrams

**Usage:** `/design <component>`

**Workflow:**
1. Analyze component
2. Create diagrams (DFD, ER, Sequence)
3. Write technical documentation
4. Save to docs/

**File Location:** `.github/skills/design.md`

---

### Skill 6: `/plan` - Task Planning

**Purpose:** Break down work into actionable tasks

**Usage:** `/plan <task description>`

**Workflow:**
1. Understand the goal
2. Identify sub-tasks
3. Estimate effort
4. Create task list

**File Location:** `.github/skills/plan.md`

---

## 8. Available Agents Reference

### Agent 1: spec-analyst

**Purpose:** Requirements analysis and implementation planning

**Triggers:** When user provides feature specs or new requirements

**Tools:** Read, Glob, Grep, WebFetch

**File:** `.github/agents/spec-analyst.md`

---

### Agent 2: feature-dev

**Purpose:** Implement PySpark code following project patterns

**Triggers:** After spec-analyst creates an approved plan

**Tools:** Read, Write, Edit, Glob, Grep, Bash

**File:** `.github/agents/feature-dev.md`

---

### Agent 3: test-runner

**Purpose:** Create and run comprehensive unit tests

**Triggers:** After feature implementation or when testing requested

**Tools:** Read, Write, Edit, Glob, Grep, Bash

**File:** `.github/agents/test-runner.md`

---

### Agent 4: reconciler

**Purpose:** Compare Databricks and SQL Server data

**Triggers:** For data validation and migration verification

**Tools:** Read, Write, Bash, Glob, Grep

**File:** `.github/agents/reconciler.md`

---

### Agent 5: doc-generator

**Purpose:** Generate technical documentation and diagrams

**Triggers:** When documentation is needed

**Tools:** Read, Write, Glob, Grep

**File:** `.github/agents/doc-generator.md`

---

## 9. Testing Your Setup

### Step 9.1: Verify Skill Registration

```bash
# Start Claude Code
claude

# Type a skill command
/spec describe my feature
```

If the skill is recognized, Claude will follow the skill's workflow.

### Step 9.2: Check for Errors

Common issues:
- **Skill not recognized:** Check frontmatter syntax (YAML must be valid)
- **Agent not used:** Check description matches use case
- **Tools not working:** Verify tool names are correct

### Step 9.3: Debug Frontmatter

Ensure your frontmatter uses valid YAML:

```yaml
---
name: skill-name           # Required: lowercase, no spaces
description: "Description" # Required: in quotes
argument-hint: "Hint"      # Optional
---
```

---

## 10. Best Practices

### For Skills

1. **Clear descriptions** - Help Claude know when to use the skill
2. **Structured workflows** - Break into phases
3. **Output templates** - Provide expected output format
4. **Examples** - Show sample interactions

### For Agents

1. **Specific roles** - Give agents a clear persona
2. **Limited scope** - Don't make agents do everything
3. **Appropriate tools** - Only include needed tools
4. **Context** - Include project-specific knowledge

### For CLAUDE.md

1. **Keep updated** - Reflect current project state
2. **Be specific** - Include actual paths, conventions
3. **Include examples** - Show code patterns
4. **List commands** - Document frequently used commands

### Git Management

Add to `.gitignore` if skills/agents contain sensitive info:
```
.claude/
.github/agents/
.github/skills/
```

Or commit them to share with team (recommended).

---

## Quick Start Checklist

- [ ] Create `.github/skills/` directory
- [ ] Create `.github/agents/` directory
- [ ] Create at least one skill file with valid frontmatter
- [ ] Create at least one agent file with valid frontmatter
- [ ] Create or update `CLAUDE.md` with project context
- [ ] Test a skill command in Claude Code
- [ ] Verify agents are being used for appropriate tasks

---

## 11. Real-World Examples from Population Advyzer Project

This section shows how skills and agents were used to investigate and fix actual production issues.

### Example 1: Bug Investigation with `/bug` Skill

**Scenario:** CHRONIC_HCC was returning NULL for Method 4 (Persistent HCC) records in the gap suspecting process.

**Invocation:**
```
/bug CHRONIC_HCC is NULL for Method 4 in suspected_gaps and risk_member_hcc tables
```

**What Claude Did:**

1. **Phase 1 - Understanding:**
   - Identified symptoms: CHRONIC_HCC = NULL when it should be 'Y'
   - Environment: DEV/QA/STG
   - Impact: High - affects downstream risk scoring

2. **Phase 2 - Investigation:**
   ```python
   # Traced code path through:
   # gap_suspecting_helper.py → method4_integration_with_suspected_gaps()
   # → union with other methods → insert_to_risk_member_hcc()
   ```
   
3. **Phase 3 - Root Cause:**
   - Method 4 pulls from `member_persistent_cc` table which doesn't have CHRONIC_HCC column
   - When unioned with other methods, NULL is used for missing columns
   - By definition, Method 4 (Persistent HCC) records ARE chronic conditions

4. **Phase 4 - Fix Implementation:**
   ```python
   # Added at line 1633 in gap_suspecting_helper.py
   if "CHRONIC_HCC" not in df_suspected_gaps_all.columns:
       df_suspected_gaps_all = df_suspected_gaps_all.withColumn(
           "CHRONIC_HCC", F.lit(None).cast("string"))

   df_suspected_gaps_all = df_suspected_gaps_all.withColumn(
       "CHRONIC_HCC",
       F.when((F.col("METHOD_ID") == 4) & F.col("CHRONIC_HCC").isNull(), F.lit("Y"))
        .otherwise(F.col("CHRONIC_HCC"))
   )
   ```

5. **Phase 5 - Validation:**
   - Dry run confirmed no unintended side effects
   - Method 4 records now correctly have CHRONIC_HCC = 'Y'

---

### Example 2: Knowledge Base Creation with `/design` Skill

**Scenario:** Create comprehensive documentation for the Risk Engine including SEDITS rules, HCC mapping, and bug fixes.

**Invocation:**
```
/design Risk Engine Knowledge Base including SEDITS, HCC-1 exclusion marker, and recent bug fixes
```

**What Claude Did:**

1. **Analyzed Components:**
   - `cms_hcc_transformations.py` - SEDITS validation rules
   - `ma_ra_model_constants.yaml` - SEDITS diagnosis codes
   - `transformations_commons.py` - HCC mapping functions

2. **Created Knowledge Base:**
   - `docs/Risk_Engine_Knowledge_Base.md` with sections:
     - SEDITS (Sex/Age Edits) Rules
     - HCC-1 Exclusion Marker concept
     - Bug fixes (CHRONIC_HCC, FREQUENCY, DURATION, Missing Claim Date)
     - Reference data changes from PR #370

3. **Added Mermaid Diagrams:**
   ```mermaid
   flowchart TD
       A[Diagnosis Code] --> B{SEDITS Check}
       B -->|Invalid for Demographics| C[HCC = -1]
       B -->|Valid| D[Normal HCC Mapping]
       C --> E[Excluded from Scoring]
       D --> F[Included in RAF Score]
   ```

---

### Example 3: Data Reconciliation with `/reconcile` Skill

**Scenario:** Compare gap suspecting results between Databricks (cloud) and SQL Server (on-prem).

**Invocation:**
```
/reconcile suspected_gaps STG
```

**What Claude Did:**

1. **Connected to Both Sources:**
   - Databricks STG via `databricks.sdk.WorkspaceClient`
   - SQL Server via `pyodbc` with Windows authentication

2. **Executed Comparison Queries:**
   ```sql
   -- Databricks
   SELECT COUNT(*), HCC_CODE, METHOD_ID 
   FROM pop_stg.plan_curation.suspected_gaps 
   GROUP BY HCC_CODE, METHOD_ID

   -- SQL Server  
   SELECT COUNT(*), HCC_CODE, METHOD_ID
   FROM PA_PROD_BCBSAR_APP_v37.dbo.SuspectedGaps
   GROUP BY HCC_CODE, METHOD_ID
   ```

3. **Generated Variance Report:**
   | Metric | Databricks | SQL Server | Variance |
   |--------|------------|------------|----------|
   | Total Rows | 125,432 | 125,418 | +14 |
   | Method 4 Rows | 18,234 | 18,220 | +14 |

4. **Identified Root Cause:**
   - 14 extra rows due to different CHRONIC_HCC handling

---

### Example 4: Spec-Driven Development with `/spec` Skill

**Scenario:** Implement confidence factor calculation based on frequency and duration.

**Invocation:**
```
/spec Add confidence factor calculation using ref_method_frequency lookup based on FREQUENCY count and DURATION days
```

**What Claude Did:**

1. **Clarified Requirements:**
   - Q: "Should FREQUENCY be counted per claim or per distinct CLAIM_BID?"
   - A: "Per distinct CLAIM_BID"
   - Q: "What's the precedence: FREQUENCY or DURATION?"
   - A: "FREQUENCY first, then DURATION within FREQUENCY bucket"

2. **Created Implementation Plan:**
   ```markdown
   ## Implementation Plan
   
   ### Files to Modify:
   1. `src/spark/gp_suspecting/gap_suspecting_helper.py`
      - Add `calculate_frequency()` function
      - Add `calculate_duration()` function
      - Add `lookup_confidence_factor()` function
   
   2. `src/sql/data/ref_method_frequency.csv`
      - Update confidence factor values
   
   ### Test Cases:
   - FREQUENCY=1, DURATION=30 → CONFIDENCE_FACTOR=0.50
   - FREQUENCY=3, DURATION=180 → CONFIDENCE_FACTOR=0.75
   - FREQUENCY=5+, DURATION=360 → CONFIDENCE_FACTOR=0.95
   ```

3. **After Approval - Implemented:**
   - Window function for distinct CLAIM_BID counting
   - Duration calculation from latest claim date
   - Join with ref_method_frequency for confidence factor

---

## 12. Creating Project-Specific Skills

### Template: Healthcare Data Investigation Skill

For healthcare data engineering projects, here's a specialized bug investigation skill:

```markdown
---
name: data-bug
description: "Healthcare data bug investigation. Use for data quality issues, missing records, incorrect calculations, or reconciliation discrepancies."
argument-hint: "Describe the data issue or paste validation query results"
---

# Data Bug Investigation

## Phase 1: Data Profiling
1. Identify affected tables and columns
2. Run profiling queries:
   - NULL counts
   - Distinct value counts
   - Min/Max/Avg values
   - Distribution analysis

## Phase 2: Lineage Tracing
1. Trace data from source to target
2. Identify transformation steps
3. Check for filtering/aggregation logic

## Phase 3: Root Cause Categories
- **Source Data Issue:** Bad data from upstream
- **Transformation Bug:** Incorrect logic in PySpark
- **Reference Data Mismatch:** Outdated lookup tables
- **Schema Drift:** Column added/removed/renamed
- **Timing Issue:** Race condition or late-arriving data

## Phase 4: Fix Approach
1. Propose fix with data backfill strategy
2. Get approval before modifying production data
3. Create validation query to confirm fix
```

---

## 13. Tips for Effective Skill Usage

### DO:
- **Be specific in your skill invocation** - Include table names, column names, error messages
- **Provide context** - Mention the environment (DEV/QA/STG/PROD)
- **Share sample data** - Paste query results or error logs
- **Trust the workflow** - Let Claude complete each phase before interrupting

### DON'T:
- **Don't skip clarification** - Answer Claude's questions thoroughly
- **Don't approve plans blindly** - Review the proposed changes
- **Don't modify code during execution** - Let Claude complete the fix
- **Don't forget validation** - Always verify the fix works

### Best Practices for Healthcare Data Projects:
1. **Never log PII/PHI** - Remind Claude of HIPAA compliance
2. **Use parameterized queries** - Prevent SQL injection
3. **Validate reference data** - Check for stale lookup tables
4. **Test edge cases** - NULL handling, empty DataFrames, type mismatches
5. **Document in knowledge base** - Create `docs/` entries for recurring issues

---

## Support

For issues with Claude Code:
- GitHub: https://github.com/anthropics/claude-code/issues
- Documentation: https://docs.anthropic.com/claude-code

---

**Document Version:** 1.1  
**Last Updated:** 2026-05-10  
**Change Log:**
- v1.1 (2026-05-10): Added real-world examples from Population Advyzer project, project-specific skill templates, tips for effective usage
