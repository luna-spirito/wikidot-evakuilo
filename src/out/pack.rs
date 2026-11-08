//! Deterministic per-page tar+zstd archives.
//!
//! The archive format is unchanged from v1: `rNNN.txt` entries in rev_no
//! order, GNU tar headers with mtime 0 / uid 0 / gid 0 / mode 0644, and one
//! single-threaded zstd stream. Identical DB rows produce byte-identical
//! archives, which is what makes content addressing useful.

use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::db::FullRevision;

/// Page identity for frontmatter rendering (everything but the revision).
pub struct PageIdentity<'a> {
    pub site: &'a str,
    pub page_id: &'a str,
    pub slug: &'a str,
    pub title: &'a str,
    pub tags: &'a [String],
}

/// v1's rN.txt frontmatter, byte for byte:
/// `---\n<yaml>\n---\n<content>\n`. Strings are double-quoted with the same
/// four escapes (`\`, `"`, newline, tab) v1's minimal YAML emitter used.
pub fn revision_txt(page: &PageIdentity<'_>, rev: &FullRevision) -> String {
    let tags = if page.tags.is_empty() {
        "[]".to_string()
    } else {
        format!(
            "[{}]",
            page.tags
                .iter()
                .map(|t| yaml_quote(t))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    format!(
        "---\n\
         title: {title}\n\
         tags: {tags}\n\
         page_id: {page_id}\n\
         site: {site}\n\
         slug: {slug}\n\
         revision: {rev_no}\n\
         revision_id: {revision_id}\n\
         author: {author}\n\
         timestamp: {ts}\n\
         ---\n\
         {content}\n",
        title = yaml_quote(page.title),
        page_id = yaml_quote(page.page_id),
        site = yaml_quote(page.site),
        slug = yaml_quote(page.slug),
        revision_id = yaml_quote(&rev.rev_id),
        rev_no = rev.rev_no,
        author = rev.author,
        ts = rev.ts,
        content = rev.content,
    )
}

pub(super) fn yaml_quote(s: &str) -> String {
    let escaped = s
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\t', "\\t");
    format!("\"{escaped}\"")
}

/// `rNNN.txt`, zero-padded to 3 like v1 (wider numbers keep their width).
fn rev_entry_name(rev_no: i64) -> String {
    format!("r{rev_no:0>3}.txt")
}

/// `pages_by_id` sharding: 2/2/rest of a zero-padded page id. Only used for
/// the pre-CAS archive location, but kept in one place so the adoption
/// reader cannot disagree with where old publications actually wrote.
fn page_id_shards(page_id: &str) -> (String, String, String) {
    let padded = format!("{page_id:0>5}");
    (
        padded[..2].to_string(),
        padded[2..4].to_string(),
        padded[4..].to_string(),
    )
}

/// Relative location of a pre-CAS page archive. New publications never
/// write there; it exists only as read-only migration input for adoption.
fn legacy_archive_ref(page_id: &str) -> String {
    let (a, b, rest) = page_id_shards(page_id);
    format!("pages_by_id/{a}/{b}/{rest}.zst")
}

/// Pre-CAS page archive location. New publications never write here; it
/// remains as read-only input for adoption.
pub fn page_archive_path(out_dir: &Path, page_id: &str) -> PathBuf {
    out_dir.join(legacy_archive_ref(page_id))
}

/// Write one page's tar+zstd stream into an already-allocated file. The
/// caller owns durability and the eventual rename into the object store.
pub fn write_archive(
    file: File,
    revisions: &[FullRevision],
    zstd_level: i32,
    render: impl Fn(&FullRevision) -> String,
) -> Result<()> {
    let mut enc = zstd::stream::Encoder::new(file, zstd_level).context("zstd encoder")?;
    {
        let mut tar = tar::Builder::new(&mut enc);
        for rev in revisions {
            let body = render(rev);
            let mut header = tar::Header::new_gnu();
            header.set_size(body.len() as u64);
            header.set_mode(0o644);
            header.set_mtime(0);
            header.set_uid(0);
            header.set_gid(0);
            tar.append_data(&mut header, rev_entry_name(rev.rev_no), body.as_bytes())?;
        }
        tar.finish().context("finishing tar")?;
    }
    let file = enc.finish().context("finishing zstd")?;
    file.sync_all().context("fsync archive")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revision_txt_matches_v1_layout() {
        let page = PageIdentity {
            site: "agiat",
            page_id: "1402564633",
            slug: "theme:magnorum-undecim",
            title: "Magnorum Undecim",
            tags: &["theme".into(), "sigma".into()],
        };
        let rev = FullRevision {
            rev_no: 20,
            rev_id: "1533147940".into(),
            ts: 1761070219,
            author: 3090772,
            content: "[[div class=\"theme\"]]\nbody text".into(),
        };
        let txt = revision_txt(&page, &rev);
        let expected = concat!(
            "---\n",
            "title: \"Magnorum Undecim\"\n",
            "tags: [\"theme\", \"sigma\"]\n",
            "page_id: \"1402564633\"\n",
            "site: \"agiat\"\n",
            "slug: \"theme:magnorum-undecim\"\n",
            "revision: 20\n",
            "revision_id: \"1533147940\"\n",
            "author: 3090772\n",
            "timestamp: 1761070219\n",
            "---\n",
            "[[div class=\"theme\"]]\nbody text\n",
        );
        assert_eq!(txt, expected);
    }

    #[test]
    fn yaml_quote_escapes_like_v1() {
        assert_eq!(yaml_quote("plain"), "\"plain\"");
        assert_eq!(yaml_quote("a\"b\\c"), "\"a\\\"b\\\\c\"");
        assert_eq!(yaml_quote("multi\nline\ttab"), "\"multi\\nline\\ttab\"");
        assert_eq!(yaml_quote(""), "\"\"");
    }

    #[test]
    fn entry_names_zero_pad_to_three() {
        assert_eq!(rev_entry_name(1), "r001.txt");
        assert_eq!(rev_entry_name(20), "r020.txt");
        assert_eq!(rev_entry_name(445), "r445.txt");
        assert_eq!(rev_entry_name(1234), "r1234.txt");
    }

    #[test]
    fn page_archives_are_deterministic_and_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let rev = |no: i64| FullRevision {
            rev_no: no,
            rev_id: format!("rev{no}"),
            ts: 1_700_000_000 + no,
            author: 42,
            content: format!("body of {no}"),
        };
        let revs = vec![rev(1), rev(2), rev(10)];
        let identity = PageIdentity {
            site: "s",
            page_id: "1234567890",
            slug: "some:page",
            title: "T",
            tags: &[],
        };
        let render = |r: &FullRevision| revision_txt(&identity, r);

        let a = dir.path().join("a.zst");
        write_archive(File::create(&a).unwrap(), &revs, 19, render).unwrap();
        let b = dir.path().join("b.zst");
        write_archive(File::create(&b).unwrap(), &revs, 19, render).unwrap();
        assert_eq!(std::fs::read(&a).unwrap(), std::fs::read(&b).unwrap());

        let dec = zstd::decode_all(std::fs::read(&a).unwrap().as_slice()).unwrap();
        let mut ar = tar::Archive::new(dec.as_slice());
        let mut names = Vec::new();
        for entry in ar.entries().unwrap() {
            names.push(
                entry
                    .unwrap()
                    .path()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        assert_eq!(names, ["r001.txt", "r002.txt", "r010.txt"]);
    }
}
