# Usage: awk -v expected_repetitions=6 -f scripts/summarize_bounded_regression.awk RESULTS/*.csv
BEGIN {
    FS = ","; OFS = "\t"
    if (!expected_configurations) expected_configurations = 24
}

function fail(message) {
    print message > "/dev/stderr"
    failed = 1
    exit 2
}

function add(metric, value) {
    counts[metric]++
    samples[metric SUBSEP counts[metric]] = value + 0
}

function median(metric,    n, i, j, value) {
    for (i in sorted) delete sorted[i]
    n = counts[metric]
    for (i = 1; i <= n; i++) sorted[i] = samples[metric SUBSEP i]
    for (i = 1; i <= n; i++) {
        for (j = i + 1; j <= n; j++) {
            if (sorted[j] < sorted[i]) {
                value = sorted[i]; sorted[i] = sorted[j]; sorted[j] = value
            }
        }
    }
    if (n % 2) return sorted[(n + 1) / 2]
    return (sorted[n / 2] + sorted[n / 2 + 1]) / 2
}

function extreme(metric, upper,    n) {
    median(metric)
    n = counts[metric]
    return sorted[upper ? n : 1]
}

FNR == 1 {
    if ($1 != "backend" || NF != 20) fail("unexpected CSV header: " FILENAME)
    next
}

{
    if (NF != 20 || ($1 != "direct-owned" && $1 != "direct-borrowed") || $10 <= 0) fail("invalid result: " FILENAME)
    if (backend && backend != $1) fail("mixed ownership semantics: " FILENAME)
    backend = $1
    version = FILENAME ~ /_baseline[.]csv$/ ? "baseline" : "candidate"
    if (version == "candidate" && FILENAME !~ /_candidate[.]csv$/) fail("unknown version: " FILENAME)
    config = $2 SUBSEP $7 SUBSEP $6
    record = config SUBSEP $8 SUBSEP version
    if (record in rates) fail("duplicate result: " FILENAME)
    pair = config SUBSEP $8
    identity = $3 SUBSEP $4 SUBSEP $5 SUBSEP $9
    if (pair in identities && identities[pair] != identity) fail("mismatched paired workload: " FILENAME)
    identities[pair] = identity
    rates[record] = $10 + 0
    if (!(config in configs)) config_count++
    configs[config] = 1
    seeds[config SUBSEP $8] = 1
    add(config SUBSEP version, $10)
}

END {
    if (failed) exit 2
    if (config_count != expected_configurations) fail("wrong workload configuration count")
    for (config in configs) {
        n = counts[config SUBSEP "baseline"]
        if (!n || n != counts[config SUBSEP "candidate"] ||
            (expected_repetitions && n != expected_repetitions)) fail("incomplete paired result set")
    }
    for (record in seeds) {
        before = rates[record SUBSEP "baseline"]
        after = rates[record SUBSEP "candidate"]
        if (!before || !after) fail("unpaired result")
        split(record, parts, SUBSEP)
        config = parts[1] SUBSEP parts[2] SUBSEP parts[3]
        add(config SUBSEP "change", 100 * (after / before - 1))
    }
    print "workload", "value_bytes", "workers", "repetitions", "baseline_mops", "candidate_mops", "paired_median_change_pct", "paired_min_change_pct", "paired_max_change_pct"
    split("resident-hit read-miss cold-fill turnover replace delete-reinsert", workloads, " ")
    split("64 65536", sizes, " ")
    for (size = 1; size <= 2; size++) {
        for (workers = 1; workers <= 2; workers++) {
            for (workload = 1; workload <= 6; workload++) {
                config = workloads[workload] SUBSEP sizes[size] SUBSEP workers
                if (!(config in configs)) continue
                print workloads[workload], sizes[size], workers, counts[config SUBSEP "baseline"], \
                    sprintf("%.3f", median(config SUBSEP "baseline")), \
                    sprintf("%.3f", median(config SUBSEP "candidate")), \
                    sprintf("%.3f", median(config SUBSEP "change")), \
                    sprintf("%.3f", extreme(config SUBSEP "change", 0)), \
                    sprintf("%.3f", extreme(config SUBSEP "change", 1))
            }
        }
    }
}
