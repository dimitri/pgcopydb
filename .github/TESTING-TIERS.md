# GitHub Actions Testing Tiers

## Overview

The tiered testing approach runs fast checks first and only proceeds to expensive tests if earlier tiers pass. This saves CI credits and provides faster feedback.

## Tier Structure

### Tier 1: Fast Checks (~2-3 minutes, 3 jobs)
**Cost:** ~6-9 CI minutes total

- **style_checker**: C code style validation
- **banned_api_checker**: Security API validation
- **docs**: Documentation build and validation

**Purpose:** Catch common mistakes immediately (syntax errors, style issues, banned APIs)

**If fails:** Stop immediately, don't run any other tests

### Tier 2: Smoke Tests (~3-5 minutes, 6 jobs)
**Cost:** ~18-30 CI minutes total
**Depends on:** Tier 1 passing

- **ci tests** (PG 16, 17, 18): Basic CI validation
- **unit tests** (PG 16, 17, 18): Unit test suite

**Purpose:** Verify basic compilation and core functionality

**If fails:** Stop before running expensive integration tests

### Tier 3: Integration Tests (~3-5 minutes each, 45 jobs)
**Cost:** ~135-225 CI minutes total
**Depends on:** Tier 1 AND Tier 2 passing

- 15 test suites × 3 PostgreSQL versions = 45 jobs
- Includes: pagila, filtering, CDC, follow, etc.

**Purpose:** Comprehensive validation of all features

**If fails:** Other integration tests continue (fail-fast: false)

## Cost Savings

### Before (All tests run on every push):
- **Total jobs:** 54 jobs (3 fast + 51 integration)
- **Total time:** ~162-270 CI minutes
- **On failure:** All jobs run regardless of early failures

### After (Tiered approach):

**Best case (Tier 1 fails):**
- **Jobs run:** 3 jobs
- **Total time:** ~6-9 CI minutes
- **Savings:** 94% fewer CI minutes

**Medium case (Tier 2 fails):**
- **Jobs run:** 9 jobs (3 + 6)
- **Total time:** ~24-39 CI minutes
- **Savings:** 76% fewer CI minutes

**Worst case (All pass or Tier 3 fails):**
- **Jobs run:** 54 jobs (same as before)
- **Total time:** ~162-270 CI minutes
- **Savings:** 0% (but you get faster feedback on failures)

## Expected Savings

Based on typical development workflows:

- **Style/syntax errors:** ~40% of pushes → 94% savings
- **Compilation/unit test failures:** ~30% of pushes → 76% savings
- **Integration test failures:** ~20% of pushes → 0% savings (but earlier feedback)
- **All tests pass:** ~10% of pushes → 0% savings

**Estimated average savings: 50-60% fewer CI minutes**

## Migration Plan

### Step 1: Test the new workflow (Recommended)

Keep both workflows temporarily:
1. Rename `run-tests.yml` to `run-tests-old.yml`
2. Rename `run-tests-tiered.yml` to `run-tests.yml`
3. Test on a few PRs
4. Once confident, delete `run-tests-old.yml`

### Step 2: Or switch immediately

```bash
mv .github/workflows/run-tests.yml .github/workflows/run-tests-old.yml.bak
mv .github/workflows/run-tests-tiered.yml .github/workflows/run-tests.yml
git add .github/workflows/
git commit -m "Switch to tiered testing workflow for cost savings"
```

## Monitoring

Check your Actions usage:
```
gh api /repos/teknogeek0/pgcopydb/actions/workflows/run-tests.yml/timing
```

Or view in GitHub:
Settings → Billing → Actions usage

## Customization

### Adjust tier membership

Move tests between tiers by editing the matrix in each job:

```yaml
# Make pagila a smoke test (Tier 2)
smoke_tests:
  matrix:
    TEST:
      - ci
      - unit
      - pagila  # Add here

# Remove from integration tests (Tier 3)
integration_tests:
  matrix:
    TEST:
      # - pagila  # Remove from here
      - pagila-multi-steps
      # ...
```

### Add fail-fast to Tier 3

If you want Tier 3 to stop on first failure:

```yaml
integration_tests:
  strategy:
    fail-fast: true  # Change from false to true
```

### Run only on main branch

Save even more by only running Tier 3 on main:

```yaml
integration_tests:
  if: github.ref == 'refs/heads/main'
  # ... rest of job
```

## Tips

1. **Monitor your most common failures** - Move frequently-failing tests to earlier tiers
2. **Balance parallelism** - Tier 1 runs sequentially, Tiers 2-3 run in parallel
3. **Use workflow_dispatch** - Manually trigger full test runs when needed
4. **Check logs regularly** - Ensure tiering doesn't mask issues
