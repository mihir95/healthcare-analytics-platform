# Databricks Cluster Selection & Personal Compute Behavior

## Context

While setting up Azure Databricks for a healthcare analytics project, 
I observed inconsistent Personal Compute behavior across two workspaces 
in the same Azure subscription and region (West US 2).

This document captures the issue, investigation, root cause, and key learnings.

---

## Problem Observed

### Workspace A (older workspace)
- Personal Compute works normally
- Standard CPU VM types selectable (D-series, DS-series)
- Default DBU: 0.75 DBU/hr
- Flexible node selection in UI

### Workspace B (new workspace — healthcare project)
- Personal Compute is restrictive
- Standard CPU VM types visible in dropdown but not selectable
- Only GPU node (Standard_NC4as_T4_v3) selectable by default
- All Purpose Unrestricted compute works normally
- Default DBU: 1.0 DBU/hr

---

## Hypotheses Investigated

### 1. Azure Quota Issue — Eliminated
Checked Azure Portal → Quotas → Compute → West US 2.

Quota showed as 0/10 for standard VM families meaning:
- 0 cores currently in use
- 10 cores available

**Important distinction:**
- Quota does NOT affect UI dropdown selection
- Quota failures happen at cluster START time, not selection time
- A cluster with insufficient quota will attempt to provision 
  then fail with error: "InsufficientQuotaError"

**Conclusion:** Not the root cause.

---

### 2. Azure Region / VM Availability — Eliminated
Some VM types have limited regional capacity independent of quota.

**Important distinction:**
- Regional capacity affects deployment success
- Does NOT explain why UI selection differs between two workspaces 
  in the same region

**Conclusion:** Not the root cause.

---

### 3. Databricks Personal Compute Policy — Root Cause

Both workspaces use the same policy family:

```json
"policy_family_id": "personal-vm"
```

However the `personal-vm` policy family is **Databricks-managed** 
meaning:
- Users can see the policy JSON
- Users can add overrides
- But Databricks enforces hidden constraints underneath
- These hidden constraints differ across workspace versions

The policy JSON showed standard VMs in the allowlist:
```json
"node_type_id": {
    "type": "allowlist",
    "values": [
        "Standard_DS3_v2",
        "Standard_DS4_v2",
        "Standard_NC4as_T4_v3"
    ]
}
```

Being in the allowlist means the policy PERMITS the node type.
But Databricks workspace-level hidden constraints can still 
PREVENT selection in the UI regardless of what the allowlist says.

This explains why:
- Node types appear in dropdown (allowlist permits them)
- But cannot be selected (hidden workspace constraint blocks them)

**Conclusion:** Primary root cause.

---

### 4. DBU Pricing Difference

Observed:
- Workspace A → 0.75 DBU/hr
- Workspace B → 1.0 DBU/hr

**Why this happens:**
DBU (Databricks Unit) pricing is determined by:
- Workspace pricing tier (Standard vs Premium)
- Compute class (Personal vs All Purpose vs Jobs)
- Databricks Runtime version selected

Personal Compute is designed to be cheaper (0.75 DBU/hr) 
because it runs Spark in local mode — single node, 
no distributed computing overhead.

The 1.0 DBU/hr on Workspace B suggests it may be running 
as All Purpose compute class internally despite the 
Personal Compute label in the UI.

**Conclusion:** Secondary observation. Confirms workspace-level 
billing configuration differs independently of node type selection.

---

## Root Cause Summary

Three independent workspace-level differences caused the behavior:

1. **Databricks-managed policy hidden constraints**
   - Same `personal-vm` policy family ≠ same behavior
   - Newer workspaces have stricter hidden enforcement
   - Allowlist controls what policy permits
   - Hidden constraints control what UI actually allows

2. **Workspace version differences**
   - Databricks continuously updates workspace defaults
   - Older workspaces retain original behavior
   - Newer workspaces get updated stricter defaults
   - No way to downgrade workspace behavior

3. **Workspace billing tier**
   - Controls DBU pricing independently
   - Affects cost but not functionality

---

## Key Learnings

### 1. Same feature name ≠ same behavior
`Personal Compute` in an older workspace behaves differently 
than in a newer workspace even in the same subscription and region.

### 2. Three distinct failure points exist
| Failure Point | Symptom | Root Cause |
|---|---|---|
| UI selection blocked | Can see but not click node | Policy/workspace constraint |
| Cluster fails to start | Pending then error | Azure quota insufficient |
| Cluster starts but fails | Runtime error | VM capacity in region |

### 3. Quota ≠ UI availability
- Quota = Azure resource allocation limit
- UI availability = Databricks policy enforcement
- These are completely independent systems

### 4. Allowlist ≠ selectable
A node type in the Databricks policy allowlist means 
the policy permits it. It does NOT guarantee the UI 
allows selection — hidden workspace constraints can 
still block it.

### 5. Unrestricted compute bypasses policy enforcement
All Purpose Unrestricted compute ignores Personal Compute 
policy restrictions entirely. If a node works in Unrestricted 
but not Personal — it is a policy issue not an Azure issue.

### 6. Personal Compute runs Spark in local mode
Personal Compute is not true distributed Spark. It runs 
Spark in local mode on a single node. This is:
- Cheaper (lower DBU)
- Sufficient for dev work on moderate datasets
- Not suitable for large scale production workloads

---

## Practical Decision Framework

When facing cluster issues in Azure Databricks:
Cannot select node in UI?
→ Policy or workspace constraint issue
→ Try All Purpose Unrestricted
Can select but cluster fails to start?
→ Check Azure quota (Portal → Quotas → Compute → your region)
→ Request quota increase if needed
Cluster starts but job fails?
→ Check regional VM capacity
→ Try different VM type or different region
DBU higher than expected?
→ Check workspace pricing tier
→ Check compute class (Personal vs All Purpose)
Cost concern on dev cluster?
→ Enable spot instances (saves 60-70%)
→ Set auto-termination to 20 minutes
→ Use single node for dev work
→ Avoid GPU nodes for ETL/DE work

---

## Final Decision for This Project

Used All Purpose Unrestricted compute with:
- Node type: Standard_DS3_v2 (14GB RAM, 4 cores)
- Single node mode
- Auto-termination: 20 minutes
- Spot instance: enabled

Reasoning:
- Personal Compute restriction prevented standard CPU selection
- All Purpose gives full control
- Single node sufficient for CMS dataset sizes
- Cost managed through spot instances and auto-termination