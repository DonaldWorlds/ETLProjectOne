# metadata/


This directory stores **system memory**, not business data.

It allows the ETL system to answer:

- What ran last?
- What changed?
- What decision was made?
- Can we prove it?

## Examples of Metadata

- File hashes
- Row counts
- Schema signatures
- Ingestion logs

## Important

- Metadata outputs are NOT committed to git
- Metadata structure and intent ARE documented