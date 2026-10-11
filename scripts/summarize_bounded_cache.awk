# Usage: awk -v expected_repetitions=6 -f scripts/summarize_bounded_cache.awk RESULTS/*.csv
# Input is the fixed-width CSV emitted by bounded_cache_probe (no quoted fields).
BEGIN {
    FS = ","; OFS = "\t"
    if (!alternate_backend) alternate_backend = "direct-borrowed"
    alternate_label = alternate_backend
    gsub(/-/, "_", alternate_label)
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

function percentile(metric, fraction,    size, item, other, temporary, position) {
    for (item in sorted) delete sorted[item]
    size = counts[metric]
    if (!size) return "na"
    for (item = 1; item <= size; item++) sorted[item] = samples[metric SUBSEP item]
    for (item = 1; item <= size; item++) {
        for (other = item + 1; other <= size; other++) {
            if (sorted[other] < sorted[item]) {
                temporary = sorted[item]; sorted[item] = sorted[other]; sorted[other] = temporary
            }
        }
    }
    if (fraction == 0.5) {
        if (size % 2) return sorted[(size + 1) / 2]
        return (sorted[size / 2] + sorted[size / 2 + 1]) / 2
    }
    position = fraction == 0 ? 1 : size
    return sorted[position]
}

function show(value) { return value == "na" ? "na" : sprintf("%.3f", value) }

FNR == 1 {
    if ($1 != "backend" || NF != 20) fail("unexpected CSV header: " FILENAME)
    next
}

{
    if (NF != 20 || $10 <= 0) fail("invalid result row: " FILENAME)
    if ($1 != "direct-owned" && $1 != "quickcache" && $1 != alternate_backend) fail("unexpected backend: " FILENAME)
    config = $2 SUBSEP $7 SUBSEP $6
    record = config SUBSEP $8 SUBSEP $1
    if (record in throughput) fail("duplicate result: " FILENAME)
    pair = config SUBSEP $8
    identity = $3 SUBSEP $4 SUBSEP $5 SUBSEP $9
    if (pair in identities && identities[pair] != identity) fail("mismatched paired workload: " FILENAME)
    identities[pair] = identity
    throughput[record] = $10 + 0
    if (!(config in seen)) config_count++
    seen[config] = 1
    seeds[config SUBSEP $8] = 1
    add(config SUBSEP $1 SUBSEP "mops", $10)
    add(config SUBSEP $1 SUBSEP "entries", $15)
    if ($12 > 0) add(config SUBSEP $1 SUBSEP "hit", 100 * $11 / $12)
}

END {
    if (failed) exit 2
    if (expected_configurations && config_count != expected_configurations) fail("missing workload configurations")
    for (config in seen) {
        number = counts[config SUBSEP "quickcache" SUBSEP "mops"]
        if (!number || counts[config SUBSEP "direct-owned" SUBSEP "mops"] != number ||
            counts[config SUBSEP alternate_backend SUBSEP "mops"] != number ||
            (expected_repetitions && number != expected_repetitions)) {
            fail("incomplete backend set or wrong repetition count")
        }
        for (seed_record in seeds) {
            split(seed_record, parts, SUBSEP)
            seed_config = parts[1] SUBSEP parts[2] SUBSEP parts[3]
            if (seed_config != config) continue
            seed = parts[4]
            reference = throughput[config SUBSEP seed SUBSEP "quickcache"]
            owned = throughput[config SUBSEP seed SUBSEP "direct-owned"]
            borrowed = throughput[config SUBSEP seed SUBSEP alternate_backend]
            if (!reference || !owned || !borrowed) fail("unpaired seed in result set")
            add(config SUBSEP "owned_pct", 100 * (owned / reference - 1))
            add(config SUBSEP "borrowed_pct", 100 * (borrowed / reference - 1))
            add(config SUBSEP "alternate_owned_pct", 100 * (borrowed / owned - 1))
        }
    }
    print "workload", "value_bytes", "workers", "repetitions", "direct_owned_mops", alternate_label "_mops", "quickcache_mops", "owned_vs_quick_paired_median_pct", alternate_label "_vs_quick_paired_median_pct", "owned_hit_pct", "quickcache_hit_pct", "owned_final_entries", "quickcache_final_entries", "owned_paired_min_pct", "owned_paired_max_pct", alternate_label "_vs_owned_paired_median_pct", "alternate_vs_owned_paired_min_pct", "alternate_vs_owned_paired_max_pct"
    split("resident-hit read-miss cold-fill turnover replace delete-reinsert", workloads, " ")
    split("64 65536", sizes, " ")
    for (size = 1; size <= 2; size++) {
        for (workers = 1; workers <= 2; workers++) {
            for (workload = 1; workload <= 6; workload++) {
                config = workloads[workload] SUBSEP sizes[size] SUBSEP workers
                if (!(config in seen)) continue
                print workloads[workload], sizes[size], workers,
                    counts[config SUBSEP "quickcache" SUBSEP "mops"],
                    show(percentile(config SUBSEP "direct-owned" SUBSEP "mops", 0.5)),
                    show(percentile(config SUBSEP alternate_backend SUBSEP "mops", 0.5)),
                    show(percentile(config SUBSEP "quickcache" SUBSEP "mops", 0.5)),
                    show(percentile(config SUBSEP "owned_pct", 0.5)),
                    show(percentile(config SUBSEP "borrowed_pct", 0.5)),
                    show(percentile(config SUBSEP "direct-owned" SUBSEP "hit", 0.5)),
                    show(percentile(config SUBSEP "quickcache" SUBSEP "hit", 0.5)),
                    show(percentile(config SUBSEP "direct-owned" SUBSEP "entries", 0.5)),
                    show(percentile(config SUBSEP "quickcache" SUBSEP "entries", 0.5)),
                    show(percentile(config SUBSEP "owned_pct", 0)),
                    show(percentile(config SUBSEP "owned_pct", 1)),
                    show(percentile(config SUBSEP "alternate_owned_pct", 0.5)),
                    show(percentile(config SUBSEP "alternate_owned_pct", 0)),
                    show(percentile(config SUBSEP "alternate_owned_pct", 1))
            }
        }
    }
}
