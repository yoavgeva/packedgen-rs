//! The published crate depends only on what `ShardedCache` needs. The
//! research components live in the unpublished `packedgen-research` crate,
//! so their dependencies never reach the cache's users.

/// The crate names of the manifest's `[dependencies]` tables, those of every
/// target included.
fn dependencies(manifest: &str) -> Vec<&str> {
    let mut names = Vec::new();
    let mut in_table = false;
    for line in manifest.lines().map(str::trim) {
        if line.starts_with('[') {
            let header = line.trim_start_matches('[').trim_end_matches(']');
            let header = header.split_once("cfg(").map_or(header, |_| {
                header.rsplit_once('.').map_or(header, |(_, table)| table)
            });
            if let Some(name) = header.strip_prefix("dependencies.") {
                names.push(name);
                in_table = false;
            } else {
                in_table = header == "dependencies";
            }
        } else if in_table && !line.is_empty() && !line.starts_with('#') {
            names.push(line.split('=').next().unwrap_or(line).trim());
        }
    }
    names
}

#[test]
fn the_published_crate_depends_only_on_parking_lot_and_foldhash() {
    let mut names = dependencies(include_str!("../Cargo.toml"));
    names.sort_unstable();
    assert_eq!(names, ["foldhash", "parking_lot"]);
}

#[test]
fn every_kind_of_dependency_table_is_read() {
    let manifest = "\
[dependencies]
# a comment
a = \"1\"

[dev-dependencies]
b = \"1\"

[target.'cfg(unix)'.dependencies]
c = \"1\"

[dependencies.d]
version = \"1\"
";
    assert_eq!(dependencies(manifest), ["a", "c", "d"]);
}
