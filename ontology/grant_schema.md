# Grant & Project Relational Database Schema

## 1. Relational Schema (5 Tables)

### 1. Grant_Project Table (Merged)
This table stores **both grant and project metadata**.

| Field | Mandatory? | Notes |
|--------|------------|------------|
| `grant_project_id` | ✅ Primary Key | Unique ID for the grant/project |
| `award_number` | ✅ Required | Funder-supplied grant/award ID |
| `DOI` | ✅ Required | DOI being registered |
| `resource` | ✅ Required | URL of the grant landing page |
| `project_title` | ✅ Required | Can store multiple titles in different languages |
| `project_description` | ❌ Optional | Project abstract/description |
| `funder_name` | ✅ Required | Name of the funder |
| `funder_id` | ✅ Required | Funder registry ID |
| `funding_type` | ✅ Required | Type of funding (grant, award, contract, etc.) |
| `funding_scheme` | ❌ Optional | Scheme for grant/award |
| `internal_award_number` | ✅ Required | Internal grant/award number |
| `start_date` | ❌ Optional | Planned start date |
| `end_date` | ❌ Optional | Planned end date |
| `amount` | ❌ Optional | Funding amount |
| `currency` | ✅ Required (if amount exists) | ISO 4217 currency code |
| `funding_percentage` | ❌ Optional | % of total funding |

---

### 2. Organization Table
This table stores **funding organizations** and **institutions affiliated with investigators**.

| Field | Mandatory? | Notes |
|--------|------------|------------|
| `organization_id` | ✅ Primary Key | Unique ID for the organization |
| `name` | ✅ Required | Name of the organization |
| `ROR` | ❌ Optional | ROR ID (for institution disambiguation) |
| `country_code` | ❌ Optional | ISO 3166-1 alpha-2 country code |

---

### 3. Investigator Table
This table stores **individual investigators**, linked to their organizations.

| Field | Mandatory? | Notes |
|--------|------------|------------|
| `investigator_id` | ✅ Primary Key | Unique ID for investigator |
| `organization_id` | ❌ Foreign Key | Links to **Organization** table (optional) |
| `role` | ✅ Required | Role (lead_investigator, co-lead, investigator) |
| `given_name` | ❌ Optional | First name |
| `family_name` | ❌ Optional | Last name |
| `alternate_name` | ❌ Optional | Alias or nickname |
| `ORCID` | ❌ Optional | ORCID ID (as URL) |

---

┌──────────────────┐  
│ Grant_Project    │  
│ (Grants + Proj.) │  
└───┬──────────────┘  
    │ 1  
    │  
    │ *  
┌───▼──────────┐  
│ Organization │  
└───┬──────────┘  
    │ 1  
    │  
    │ *  
┌───▼──────────┐  
│ Investigator │  
└──────────────┘  