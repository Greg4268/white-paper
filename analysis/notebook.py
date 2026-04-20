import marimo

__generated_with = "0.21.1"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Flock Surveillance System — Access Log Analysis

    ## Overview
    This notebook performs an exploratory data analysis (EDA) of law enforcement access logs
    for a Flock Safety automated license plate reader (ALPR) surveillance network. The analysis
    examines how agencies utilize the system and the degree to which searches adhere to
    governance standards, specifically the requirement to log a valid reason and/or case number
    for each query.

    ## Data Provenance
    - **Source:** Access logs obtained via public records request (FOIA)
    - **Format:** CSV exports from the Flock Safety agency portal
    - **Path:** `data/access_logs/` (one or more CSV files, combined via diagonal concat)
    - **Coverage:** All log entries present in the exported files
    - **Modification:** Raw files are loaded as-is; only column name normalization (strip + lowercase)
      and reason-field whitespace normalization are applied prior to analysis. No records are
      dropped during ingestion.

    ## Methodology Notes
    - **Categorization is researcher-defined.** The labels *concerning*, *legitimate*, *ambiguous*,
      etc. are operational classifications based on the presence or absence of a clear investigative
      nexus. They are not legal determinations and should not be interpreted as such.
    - **Priority order matters.** When a reason string matches patterns in multiple categories,
      the highest-priority category wins (see categorization cell for full priority chain).
      Concerning patterns are evaluated before legitimate ones to surface the most policy-relevant
      edge cases for manual review.
    - **Text prompt searches** (free-text vehicle description queries) are treated separately from
      the `reason` field. Records with a text prompt but no reason field are not re-categorized
      based on prompt content; this is a conservative choice that may undercount concerning searches.
    - **No deduplication is applied.** Each row in the access log is treated as a distinct access
      event. Repeated queries by the same officer (e.g., system retries) would inflate counts.
      Analysts should be aware of this when interpreting officer-level statistics.
    """)
    return


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from pathlib import Path
    from collections import defaultdict
    from statsmodels.stats.proportion import proportion_confint
    import matplotlib.pyplot as plt
    import math
    import re
    # ---------------------------------------------------------------------------
    # Polars display configuration
    # ---------------------------------------------------------------------------
    pl.Config.set_tbl_rows(-1)                   # Show all rows
    pl.Config.set_tbl_cols(-1)                   # Show all columns
    pl.Config.set_fmt_str_lengths(100)           # Max characters shown per string cell
    pl.Config.set_tbl_hide_dataframe_shape(True) # hide shape when printing dataframes 
    return Path, mo, pl, plt, proportion_confint, re


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Data Ingestion & Schema Validation

    All CSV files under `data/access_logs/` are loaded and combined into a single DataFrame.
    Column names are normalized (stripped of whitespace, lowercased) to handle minor formatting
    inconsistencies across export batches.

    A **diagonal concat** (`how="diagonal"`) is used, which fills any missing columns across
    files with `null` rather than raising an error. This is intentional to support lossless
    combination of files that may have slightly different column sets across export periods.
    However, schema mismatches are explicitly detected and reported for awareness of any
    structural differences between source files.
    """)
    return


@app.cell
def _(Path):
    access_logs_path = Path("../../white-paper-lfs/data/access_logs/") # white-paper-lfs repo must be cloned adjacent to white-paper repo, otherwise modify with custom path 
    access_logs_files_list = [file for file in access_logs_path.rglob("*") if file.is_file() and file.suffix == ".csv"]
    print(f"Files found: {len(access_logs_files_list)}")
    #for _f in access_logs_files_list:
        #print(f"  {_f}")
    return (access_logs_files_list,)


@app.cell
def _(access_logs_files_list, pl):
    lfs = [pl.scan_csv(file, infer_schema_length=0) for file in access_logs_files_list]
    # Normalize column names: strip surrounding whitespace and lowercase.
    # This handles common FOIA export inconsistencies like "Reason " vs "reason".
    standardized_lfs = []
    for _lf in lfs:
        col_names = _lf.collect_schema().names()
        _lf = _lf.rename({col: col.strip().lower() for col in col_names})
        col_names = _lf.collect_schema().names()
        # Drop unnamed columns (empty string after stripping)
        unnamed = [c for c in col_names if c == ""]
        if unnamed:
            _lf = _lf.drop(unnamed)

        # Drop any remaining _duplicated_* columns
        duplicated = [c for c in col_names if c.startswith("_duplicated_")]
        if duplicated:
            _lf = _lf.drop(duplicated)

        not_needed = [n for n in col_names if n in ("search date", "license plate", "id", "search type", "case .")]
        if not_needed: 
            _lf = _lf.drop(not_needed)

        # Coalesce case columns
        case_cols = [c for c in col_names if c in ("case #", "case number")]
        if len(case_cols) > 1:
            _lf = _lf.with_columns(
                pl.coalesce([pl.col(c) for c in case_cols]).alias("case_number")
            ).drop(case_cols)
        elif case_cols:
            _lf = _lf.rename({case_cols[0]: "case_number"})

        # Coalesce user columns
        user_cols = [u for u in col_names if u in ("name", "user id", "deputy user")]
        if len(user_cols) > 1:
            _lf = _lf.with_columns(
                pl.coalesce([pl.col(u) for u in user_cols]).alias("user_id")
            ).drop([u for u in user_cols])
        elif len(user_cols) == 1:
            _lf = _lf.rename({user_cols[0]: "user_id"})

        standardized_lfs.append(_lf)
    # ---------------------------------------------------------------------------
    # Schema validation — warn if files have mismatched column sets.
    # Diagonal concat will fill missing columns with null
    # ---------------------------------------------------------------------------
    all_schemas = [frozenset(_lf.collect_schema().names()) for _lf in standardized_lfs]
    unique_schemas = set(all_schemas)
    if len(unique_schemas) > 1:
        print("\n⚠ WARNING: Schema mismatch detected across source files.")
        print("  Columns present in some files but not others will be null for those files.")
        reference = set(standardized_lfs[0].collect_schema().names())
        for i, _lf in enumerate(standardized_lfs):
            diff = reference.symmetric_difference(set(_lf.collect_schema().names()))
            if diff:
                print(f"  File {i} ({access_logs_files_list[i].name}): differing columns → {diff}")
    else:
        print("\n✓ All source files share the same schema.")
    # Combine all files into a single DataFrame.
    # how="diagonal" ensures no rows are dropped even if columns differ across files.
    standardized_lfs = [_lf for _lf in standardized_lfs if _lf is not None]
    if not standardized_lfs:
        raise ValueError("No valid LazyFrames to concat — all were filtered out")
    lf = pl.concat(standardized_lfs, how="diagonal")
    # Normalize the reason field: strip leading/trailing whitespace and lowercase.
    lf = lf.with_columns([
        pl.col("reason").str.strip_chars().str.to_lowercase().alias("reason"),

        pl.col('time frame')
            .str.extract(r'^(.+?)\s+to\s+', 1)
            .str.to_datetime(format='%m/%d/%Y, %I:%M:%S %p UTC', strict=False)
            .alias('start_of_search_window'),

        pl.col('time frame')
            .str.extract(r'\s+to\s+(.+)$', 1)
            .str.to_datetime(format='%m/%d/%Y, %I:%M:%S %p UTC', strict=False)
            .alias('end_of_search_window'),
    ]).with_columns([
        (pl.col('end_of_search_window') - pl.col('start_of_search_window'))
            .dt.total_hours()
            .truediv(24)
            .alias('window_days'),
    ])
    return (lf,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Data Dictionary

    The table below documents each field in the access log export. Fields marked *self-reported*
    are entered by the searching officer at query time and are the primary subject of this analysis.

    | Column | Description | Type | Notes |
    |---|---|---|---|
    | `user_id` | Searching officer's name, initials, or unique identifier | string | Self-reported or System-assigned |
    | `org name` | Name of the agency or organization (e.g., Wheaton IL PD) | string | System-assigned |
    | `total networks searched` | Number of external networks queried via data-sharing agreements | numeric | Optional |
    | `total devices searched` | Number of camera devices queried within the search | numeric | |
    | `time frame` | Time window of the search as specified by the officer | string | Self-reported |
    | `reason` | Officer-provided justification for the search | string | **Self-reported — primary analysis field** |
    | `case_number` | Case number associated with the investigation | string | Self-reported; may be null |
    | `filters` | Any filters applied to the search query | string | |
    | `search time` | Timestamp when the query was executed | datetime | System-generated |
    | `text prompt` | Free-text descriptive search (e.g., "yellow truck, mismatched wheels") | string | Self-reported; distinct from `reason` |
    | `moderation` | Moderation flag or status | string | |
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Column Independence: `reason` vs. `text prompt`

    These are two distinct self-reported fields:

    - **`reason`**: A structured or free-text justification for why the officer is conducting
      the search (e.g., "stolen vehicle", "missing person", "warrant"). This is the governance-
      compliance field — it is not currently known what percent of agencies require reason by policy.
    - **`text prompt`**: A descriptive vehicle-attribute search (e.g., "blue sedan, cracked
      windshield"). This is believed to be a Flock feature that searches the ALPR database by vehicle
      characteristics rather than a specific plate.

    The cell below confirms that both fields can be simultaneously non-null, establishing
    they are independent columns and not alternate encodings of the same field.
    """)
    return


@app.cell
def _(lf, pl):
    # ---------------------------------------------------------------------------
    # Confirm that 'reason' and 'text prompt' are independent fields.
    # If entries exist where both are non-null, we can be confident they represent
    # different data points and should not be collapsed or treated as duplicates.
    # ---------------------------------------------------------------------------
    _count = (
        lf.filter(pl.col("reason").is_not_null() & pl.col("text prompt").is_not_null())
        .select(pl.len())
        .collect()
        .item()
    )
    print(f"Records with BOTH reason and text prompt: {_count:,}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Redacted / Null 'Reason' Column
    Are there any datasets where the reason column is empty or redacted for a majority of the dataset?
    If so, that would imply that data may be missing or be masked in the dataset and we should opt to exclude it from analysis.
    1. Read each dataset and store percent of null and redacted reason columns as percent of dataset height.
    2. Plot the distribution of percentages
    3. Determine what majority would be appropriate to begin excluding datasets at (ex. Simple Majority or >50%)
    """)
    return


@app.cell
def _(access_logs_files_list, pl, plt):
    # get the distribution of null reason column by df 
    null_dist = {} 
    redacted_dist = {} 
    for _file in access_logs_files_list:
        df = pl.read_csv(_file, infer_schema_length=0)
        _col_names = df.collect_schema().names()
        df = df.rename({col: col.strip().lower() for col in _col_names})
        df.with_columns(pl.col("reason").str.strip_chars().str.to_lowercase().alias("reason"))

        _percent_redacted = round(((df.filter(pl.col('reason').str.to_lowercase().str.contains('redacted')).height) / (df.height)) * 100, 3)
        _percent_null = round(((df.filter(pl.col('reason').is_null()).height) / (df.height)) * 100,3)

        redacted_dist[_file] = _percent_redacted 
        null_dist[_file] = _percent_null

        if _percent_null > 50:
            print(f"reason code is null for more than 50% of entries in file: {_file}")
        if _percent_redacted > 50: 
            print(f"reason code 'redacted for more than 50% of entries in file: {_file}'")

    # excluded datasets: 
    # 1. Tonawanda, NY network -> 95%+ null  
    # 2. Denver, CO -> 99%+ null 
    # 3. San Jose, CA -> all reasons were vast majority redacted (90-100%) 

    # plot distribution 
    # 1. Null percent 
    values = list(null_dist.values())

    plt.scatter(sorted(values),range(len(values)))
    plt.xscale("log")
    plt.title("Null Percent By Dataset")
    plt.xlabel("Percent Null (log scale)")

    plt.show()

    # 2. Redacted percent 
    values2 = list(redacted_dist.values())

    # plt.hist(values2, bins=5, edgecolor='black')
    # plt.title("Distribution of Redacted % by Dataset")
    # plt.xlabel("Percent of Dataset That Are Redacted")
    # plt.ylabel("Frequency")

    plt.scatter(sorted(values2),range(len(values2)))
    plt.xscale("log")
    plt.title("Redacted Percent By Dataset")
    plt.xlabel("Percent Redacted (log scale)")

    plt.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Raw Reason Frequency (Unfiltered)

    This table shows the most common raw values in the `reason`
    """)
    return


@app.cell
def _(lf, pl):
    TOTAL_SEARCHES = lf.select(pl.len()).collect().item()

    df_all = (
        lf.group_by("reason")
        .agg(
            pl.col("search time").count().alias("occurrences"),
        )
        .with_columns(
            (pl.col("occurrences") / TOTAL_SEARCHES * 100).round(2).alias("percent_of_total")
        )
        .sort(by="occurrences", descending=True)
    )

    print(f"Total searches in dataset: {TOTAL_SEARCHES:,}")
    print(f"\nTop 15 most common raw reason strings:\n")
    print(df_all.collect().head(15))
    return (TOTAL_SEARCHES,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Case Number Coverage — Overall and Agency-Specific

    This section measures the rate at which searches are logged with a case number,
    a key governance compliance indicator. Agencies using Flock are generally expected
    to associate searches with an active case; searches without a case number represent
    either reasonable suspicion of a crime being investigated or non-law enforcement use
    which would be a violation of policy.

    Results are reported for:
    1. **All agencies** (including out-of-network partners with data-sharing agreements)
    2. **Wheaton Police Department only** (a good sample agency with data spanning multiple years)

    ### Wheaton Org Name Variants
    The `WHEATON_ORGS` list below defines all known `org name` values that correspond
    to the Wheaton Police Department as they appear in the raw data. This list was
    derived by inspecting `df['org name'].unique()`. If new export batches introduce
    additional variants, this list must be updated.
    """)
    return


@app.cell
def _(TOTAL_SEARCHES, lf, pl):
    # All known org name variants for Wheaton PD as they appear in the raw data.
    # Run lf.select(pl.col("org name").unique()).collect() to verify this list when using new
    WHEATON_ORGS = ["Wheaton IL PD", "Wheaton"]

    # ---------------------------------------------------------------------------
    # Helper: a search is considered to be missing a case number if the 'case_number'
    # column is null OR contains only whitespace/empty string. Importantly, we are not 
    # checking 'reason' column for case numbers and focusing on the dedicated column 
    # ---------------------------------------------------------------------------
    def is_case_missing() -> pl.Expr:
        return pl.col("case_number").is_null() | (pl.col("case_number").str.strip_chars() == "")

    # === OVERALL STATISTICS (All Agencies, Wheaton Inclusive) ===
    without_case_number = lf.filter(is_case_missing()).select(pl.len()).collect().item()
    percent_without_case = (without_case_number / TOTAL_SEARCHES) * 100

    # === WHEATON-SPECIFIC STATISTICS ===
    wheaton_lf = lf.filter(pl.col("org name").is_in(WHEATON_ORGS))
    total_wheaton_searches = wheaton_lf.select(pl.len()).collect().item()

    wheaton_without_case_count = wheaton_lf.filter(is_case_missing()).select(pl.len()).collect().item()
    wheaton_percent_without_case = (wheaton_without_case_count / total_wheaton_searches) * 100

    print("\nTOTAL (All In- and Out-of-Network Agencies, Wheaton Inclusive)")
    print("=" * 45)
    print(f"  Total searches:              {TOTAL_SEARCHES:,}")
    print(f"  Searches WITH case #:        {TOTAL_SEARCHES - without_case_number:,}") 
    print(f"  Searches WITHOUT case #:     {without_case_number:,}")
    print(f"  % without case #:            {percent_without_case:.2f}%")

    print("\nWHEATON POLICE DEPARTMENT ONLY")
    print("=" * 45)
    print(f"  Total searches:              {total_wheaton_searches:,}")
    print(f"  Searches WITH case #:        {total_wheaton_searches - wheaton_without_case_count:,}")
    print(f"  Searches WITHOUT case #:     {wheaton_without_case_count:,}")
    print(f"  % without case #:            {wheaton_percent_without_case:.2f}%")
    return is_case_missing, total_wheaton_searches, wheaton_without_case_count


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Reason Categorization

    Each search record is assigned to one of the following researcher-defined categories
    based on the content of its `reason` field. Categories are evaluated in strict priority
    order — the first matching category wins.

    ### Priority Conflict Resolution: `concerning` vs. `legitimate`

    The `concerning` category is evaluated **before** `legitimate` by design. This means a
    reason string that contains both a concerning term and a legitimate crime type will be
    classified as `concerning`. This is a conservative choice that errs toward surfacing
    potential policy violations for manual review rather than suppressing them.

    **Known conflict — `surveillance`:** The generic term `"surveillance"` is in the
    `concerning` list. However, compound phrases like `"street crimes surveillance"` and
    `"surveillance drug house"` represent legitimate investigative contexts. These compound
    forms are also listed in `legitimate`, but since `concerning` fires first, they will be
    classified as `concerning`. Analysts should note this when reviewing the concerning
    category and may wish to manually reclassify these compound forms.

    **Known conflict — `tbd`:** The bare token `\btbd\b` is in `concerning` (no reason given).
    However, agency-specific compound codes that happen to include "tbd" as a placeholder
    (e.g., `"khp-tbd"`, `"atl 24 tbd"`, `"tbd sstf dealing"`) appear in `legitimate`.
    Because `concerning` fires first, these compound codes will be classified as `concerning`.
    The counts involved are expected to be small, but analysts should be aware of this edge
    case when reporting on the `concerning` category.

    ### Taxonomy Completeness

    The `uncategorized` count at the bottom of the distribution table represents the taxonomy's
    coverage gap. All uncategorized reasons should be reviewed and either added to an existing
    category or used to define new categories in future iterations of this analysis.
    """)
    return


@app.cell
def _(lf, pl, re):
    # ---------------------------------------------------------------------------
    # Pattern taxonomy
    # ---------------------------------------------------------------------------
    # Each entry is a dict with:
    #   "pattern" : a regex string (applied case-insensitively)
    #   "note"    : a brief explanation of why the pattern belongs in this category
    #
    # CATEGORY RATIONALE:
    #
    # CONCERNING — patterns evaluated FIRST.
    #   Includes: First Amendment-sensitive topics (political activity, protests),
    #   phrases that suggest non-investigative or speculative access ("slack search",
    #   "daytime search for best result"), and bare "surveillance" which lacks an
    #   investigative nexus without a qualifying crime type. "Background check" and
    #   "image download" are included here because they suggest uses of the system
    #   beyond active criminal investigation, which may exceed the scope of most
    #   agency acceptable use policies.
    #
    # LEGITIMATE — patterns evaluated SECOND.
    #   Includes: named crime types (homicide, theft, DUI, etc.), named incident
    #   types (missing person, welfare check, amber alert), warrant/wanted-person
    #   searches, and specific threat phrases that name a target or crime context.
    #   Generic terms that overlap with concerning (e.g., bare "threats") are
    #   intentionally excluded here to preserve the concerning category's priority.
    #
    # AMBIGUOUS — patterns evaluated FOURTH (after case_number).
    #   Includes: terms that name a broad category without sufficient specificity
    #   ("felony", "investigation", "drugs") and institutional abbreviations whose
    #   meaning is unclear from the field value alone (e.g., "ci", "rtcc").
    #   These are not necessarily improper but cannot be confidently classified
    #   without additional context.
    # ---------------------------------------------------------------------------

    PATTERN_GROUPS: dict[str, list[dict]] = {
        "concerning": [
            {"pattern": r"political",                          "note": "Political surveillance"},
            {"pattern": r"activist",                           "note": "Activist surveillance"},
            {"pattern": r"\b(protest|demonstrat\w*|rally)\b",  "note": "Protest related"},
            {"pattern": r"slack search",                       "note": "Casual non-investigative search"},
            {"pattern": r"background check",                   "note": "Outside ALPR scope without case attached"},
            {"pattern": r"image download",                     "note": "Potential data misuse - typically stored as evidence, no case # attached = high conern."}, 
            {"pattern": r'^\s*surveillance\s*$',               "note": "Generic surveillance"},
            {"pattern": r"^image?$",                         "note": "download?"},
            {"pattern": r"^download?$",                         "note": "download?"},
            {"pattern": r"^threats?$",                         "note": "Generic threat"},
            {"pattern": r"^monitor?$",                         "note": "Generic monitoring"},
            {"pattern": r"^tracking?$",                         "note": "Generic tracking"},
            {"pattern": r"pattern\s*of\s*life",                "note": "Pattern of life — crime context checked in post-processing override"},
            {"pattern": r'^(dhs|ice|cbp|atf)?\s*tbd\s*$',      "note": "Reason deferred or omitted"}, # ambiguous, tbd by itself may be concerning
        ],

        "legitimate": [
            # copy all 'concerning' reasons into legitimate to get picked up through a second pass. Anything that is picked up can be assumed to have a case number attached since the previous gate for the concerning filter did not grab it
            {"pattern": r"political",                          "note": "Political surveillance"},
            {"pattern": r"activist",                           "note": "Activist surveillance"},
            {"pattern": r"\b(protest|demonstrat\w*|rally)\b",  "note": "Protest related"},
            {"pattern": r"slack search",                       "note": "Casual non-investigative search"},
            {"pattern": r"background check",                   "note": "Outside ALPR scope without case attached"},
            {"pattern": r"image download",                     "note": "Potential data misuse - typically stored as evidence, no case # attached = high conern."}, 
            {"pattern": r'^\s*surveillance\s*$',               "note": "Generic surveillance"},
            {"pattern": r"^image?$",                         "note": "download?"},
            {"pattern": r"^download?$",                         "note": "download?"},
            {"pattern": r"^threats?$",                         "note": "Generic threat"},
            {"pattern": r"^monitor?$",                         "note": "Generic monitoring"},
            {"pattern": r"^tracking?$",                         "note": "Generic tracking"},
            {"pattern": r"pattern\s*of\s*life",                "note": "Pattern of life — crime context checked in post-processing override"},
            {"pattern": r'^(dhs|ice|cbp|atf)?\s*tbd\s*$',      "note": "Reason deferred or omitted"}, # ambiguous, tbd by itself may be concerning

            # --- The rest ---
            {"pattern": r"hit\s*(?:and|&|n|an)\s*run|\bh(?:&r|nr)\b",  "note": "Hit and run variants"},
            {"pattern": r'hit\s+run',                                  "note": "edge case hit and run"},
            {"pattern": r'h and r',                                    "note": "edge case hit and run"},
            {"pattern": r"battery|assault",                            "note": "Battery/assault — violent crime"},
            {"pattern": r"carjack(?:ing|ed)?|car\s*jack(?:ing|ed)?",   "note": "Carjacking variants incl. carjacked, car jack"},  
            {"pattern": r"(?:armed\s+)?robbery|agg\s*rob",             "note": "Robbery variants"},
            {"pattern": r"homici?de.*|homiocide|homcide",              "note": "Homicide — violent crime incl. typos"},  
            {"pattern": r"murder",                                     "note": "Murder — violent crime"},
            {"pattern": r"shoot(?:ing)?|shots?\s*fired|\bshots\b",     "note": "Shooting/shots fired"},
            {"pattern": r"sex\s*assault|rape",                         "note": "Sexual assault — violent crime"},
            {"pattern": r"abduction|kidnap.*|hijack(?:ed)?",           "note": "Abduction/kidnapping/hijacking"},  
            {"pattern": r"arson.*",                                    "note": "Arson — violent/property crime"},
            {"pattern": r"child\s*abuse|cruelty\s*to\s*(?:child(?:ren)?)", "note": "Child abuse/cruelty — violent crime"},  
            {"pattern": r"agg\s*discharge",                            "note": "Aggravated discharge of firearm"},
            {"pattern": r"stabbing",                                   "note": "Stabbing — violent crime"},
            {"pattern": r"home\s*invasion",                            "note": "Home invasion — violent crime"},
            {"pattern": r"armed\s*subject",                            "note": "Armed subject — weapons/violent context"}, 
            {"pattern": r"retail\s*theft|shoplifting?|shoplift|stealing|\bsteal\b|\bthef\b|\btheft\b", "note": "Theft variants incl. shoplift"}, 
            {"pattern": r"stolen.*|\bstole\b|\bstol\b|\budaa\b|\bupsmv\b|motor\s*vehicle\s*theft|\bmvt\b|stolen\s*(?:vehicle|mv)|\bvtheft\b|\bstl\s*veh\b", "note": "Stolen vehicle/property variants incl. vtheft, stl veh"}, 
            {"pattern": r"burg.*|\bbtmv\b",                            "note": "Burglary variants"},
            {"pattern": r"larc.*|\blarc\b",                            "note": "Larceny variants"},
            {"pattern": r"vandalism|malicious\s*mischief|tagging",     "note": "Vandalism/malicious mischief/tagging"},  
            {"pattern": r"fraud|forgery|counterfeit",                  "note": "Financial/document crimes"},  
            {"pattern": r"trespass",                                   "note": "Trespass"},
            {"pattern": r"\buucc\b",                                   "note": "Unlawful use of credit card"},
            {"pattern": r"financial\s*crime|money\s*launder(?:ing)?",  "note": "Financial crime incl. money laundering"},  
            {"pattern": r"porch\s*pirate",                             "note": "Porch pirate — package theft"},  
            {"pattern": r"illegal\s*dump(?:ing)?|litter(?:ing)?",      "note": "Illegal dumping/littering"},  
            {"pattern": r"bribery",                                    "note": "Bribery"},  
            {"pattern": r"dog\s*fight(?:ing)?",                        "note": "Dog fighting — animal cruelty"},  
            {"pattern": r"street\s*takeover",                          "note": "Street takeover"},  
            {"pattern": r"prostitution",                               "note": "Prostitution"},  
            {"pattern": r"\bcsam\b",                                   "note": "CSAM — child sexual abuse material"},  
            {"pattern": r"indecent\s*exposure",                        "note": "Indecent exposure"},  
            {"pattern": r"\bgun\b|\bweapons?\b|\buuw\b|\bcwb\b",       "note": "Weapons variants"},
            {"pattern": r"\bdui.*",                                    "note": "DUI"},
            {"pattern": r"traff.*",                                    "note": "Traffic incident"},
            {"pattern": r"flee(?:ing)?|\bflee\b|\bdwls\b",             "note": "Fleeing/eluding variants"},
            {"pattern": r"pursuit",                                    "note": "Vehicle pursuit"},
            {"pattern": r"crash|\baccident\b",                         "note": "Crash/accident"},
            {"pattern": r"\belude\b|\bevade\b",                        "note": "Eluding/evading"},
            {"pattern": r"missing\s*(?:person|juvenile|endangered)?|\bmissing\b|\bmissin\b", "note": "Missing person variants incl. typo missin"},  
            {"pattern": r"domestic(?:\s*battery)?",                    "note": "Domestic incident/battery"},
            {"pattern": r"welfare\s*check|check\s*well\s*being|\bwelfare\b",     "note": "Welfare check variants"},
            {"pattern": r"suicidal",                                   "note": "Suicidal — crisis intervention"},
            {"pattern": r"sex\s*offender",                             "note": "Sex offender"},
            {"pattern": r"death\s*inv",                                "note": "Death investigation"},
            {"pattern": r"amber\s*alert",                              "note": "AMBER Alert"},
            {"pattern": r"stalking",                                   "note": "Stalking"},
            {"pattern": r"order\s*violation|protection\s*order",       "note": "Order/protection order violation"},  
            {"pattern": r"smuggl",                                     "note": "Smuggling"},
            {"pattern": r"disturbance",                                "note": "Disturbance"},
            {"pattern": r"elderly\s*exploit(?:ation)?",                "note": "Elderly exploitation"}, 
            {"pattern": r"obstruct(?:ing)?\s*justice",                 "note": "Obstructing justice"}, 
            {"pattern": r"\bwarr(?:e?a?n?t?s?|nts?|at)\b|\bwrnt s?\b", "note": "Warrant/misspellings (warrant, warrents, warrat, wrnt, wrnts)"},
            {"pattern": r"\bwanted\b|wanted\s*person|\bfugitive\b|\bbolo\b",  "note": "Wanted/fugitive variants"},
            {"pattern": r"\bp(?:oi|voi)\b",                                   "note": "Person/vehicle of interest"},
            {"pattern": r"\bmeth\b",                                   "note": "Methamphetamine"},
            {"pattern": r"narc(?:otics?|s)?|narcotic",                 "note": "Narcotics variants"},
            {"pattern": r"drugs.*",                                    "note": "Drugs variants"},
            {"pattern": r"interdiction",                               "note": "Interdiction"},
            {"pattern": r"tbd\s*drug(?:\s*dealing)?",                  "note": "Drug TBD code — compound form"},
            {"pattern": r"terroris[tm]ic?\s*threats?|terrorist\s*threats?|terr\s*threats?|terroistic\s*threat", "note": "Terroristic threat variants"},
            {"pattern": r"threats?\s*to\s*(phh|self|public|citizens?|le|life|kill|harm|governor|airport)", "note": "Threat with named target"},
            {"pattern": r"uspis\s*threat\s*investigation|isp\s*threat\s*to\s*state\s*employee", "note": "Agency-specific threat investigations"},
            {"pattern": r"threats?\s*inv|threat\s*from\s*vehicle|safety\s*threat|judicial\s*threat", "note": "Threat investigation variants"},
            {"pattern": r"bomb\s*threats?|felony\s*threats?|felnoy\s*threats?", "note": "Bomb/felony threat variants"},
            {"pattern": r"homicidal\s*threats?|death\s*threats?|criminal\s*threat", "note": "Homicidal/death/criminal threat"},
            {"pattern": r"suicide\s*threats?|threats?\s*of\s*suicide|threatening\s*suicide", "note": "Suicide threat variants"},
            {"pattern": r"threats?/officer\s*safety|leo\s*threats?",   "note": "Officer/LEO safety threat"},
            {"pattern": r"made\s*threats?-pi|making\s*threats?",       "note": "Making threats variants"},
            {"pattern": r"school\s*threats?|event\s*threats?",         "note": "School/event threat"},
            {"pattern": r"dv\s*threats?\s*to\s*kill|gang\s*related\s*threats?", "note": "DV/gang threat"},
            {"pattern": r"verbal\s*threats?|phone\s*threats?|communicating\s*threats?", "note": "Verbal/phone/communicating threats"},
            {"pattern": r"threats?/(?:harassment|eluding)",            "note": "Threat combination variants"},
            {"pattern": r"ro\s*10-99\s*threats?|32\s*threats",         "note": "Radio code threat variants"},
            {"pattern": r"epo\s*service\s*threats?|threats?\s*to\s*kidnap|threat\s*to\s*life", "note": "EPO/kidnap/life threat"},
            {"pattern": r"made\s*threatening\s*statements?",           "note": "Threatening statements"},
            {"pattern": r"brookhollow\s*threat|no\s*kings\s*rock\s*threat|24-wayfair\s*threat", "note": "Location-specific threats"},
            {"pattern": r"shooing\s*threats?",                         "note": "Likely typo of shooting threats"},
            {"pattern": r"\bltl\b|\bicc\b|\bicc\s*threat\b",           "note": "LTL/ICC threat variants"},
            {"pattern": r"surveillance\s*drug\s*house|street\s*crimes\s*surveillance", "note": "Specific surveillance — investigative context"},
            {"pattern": r"kbi24-tbd|s25-tbd|upt-tbd-24|khp-tbd|atl\s*24\s*tbd|tbd\s*sstf\s*dealing|24ia-tbd\s*investigation\s*katzman", "note": "Agency TBD compound codes"},
            {"pattern": r"\bgang\b",                                   "note": "Gang investigation"},
            {"pattern": r"\bvoop\b|\bcitco\b",                         "note": "Order of protection/citation violation"},
            {"pattern": r"training",                                   "note": "Training"},
            {"pattern": r"trend\s*analysis|tukwila\s*incident",        "note": "Specific investigative references"},
            {"pattern": r"\b(?:blair|fred)\b",                         "note": "Case-specific named references"},
            {"pattern": r'.*dispatch quick search.*',                  "note": "these are descriptive and legitimate"},
            {"pattern": r'parcel thefts',                              "note": "Crime specified"},
            {"pattern": r"\bvucsa\b",                                  "note": "VUCSA — Violation of Uniform Controlled Substance Act"},
            {"pattern": r"\bdope\b",                                   "note": "Dope — narcotics slang"},
            {"pattern": r"\buumv\b",                                   "note": "UUMV — Unauthorized Use of a Motor Vehicle"},
            {"pattern": r"jugging",                                    "note": "Jugging — robbery after cash withdrawal"},
            {"pattern": r"agg\s*batt(?:ery)?",                         "note": "Aggravated battery"},
            {"pattern": r"road\s*rage",                                "note": "Road rage — violent driving incident"},
            {"pattern": r"\bicac\b",                                   "note": "ICAC — Internet Crimes Against Children"},
            {"pattern": r"silver\s*alert",                             "note": "Silver Alert — missing elderly/vulnerable adult"},
            {"pattern": r"welf(?:are)?\s*check",                       "note": "Welfare check variant"},
            {"pattern": r"\brunaway\b",                                "note": "Runaway — missing juvenile"},
            {"pattern": r"\bsuicide\b",                                "note": "suicide"},
            {"pattern": r"\bsig(?:nal)?\s*(?:\d+|[a-z]\b|\d+[a-z])", "note": "Signal/sig + code — radio/dispatch code with context"},
            {"pattern": r"\bsuicide\b",                                "note": "Suicide — crisis/death investigation"},
            {"pattern": r"\bstln\b",                                   "note": "STLN — stolen abbreviation"},
            {"pattern": r"\bthefts\b",                                 "note": "Thefts — plural theft variant"},
            {"pattern": r"\bstolo\b",                                  "note": "Stolo — stolen vehicle slang"},
            {"pattern": r"\bfelon\b(?:\s*\w+)+",                      "note": "Felon with context — e.g. felon in possession"},
            {"pattern": r"\bdemo\b",                                   "note": "Demo — demonstration/incident"},
            {"pattern": r"\bchase\b",                                  "note": "Chase — vehicle pursuit"},
            {"pattern": r"\binterdict\b",                              "note": "Interdict — interdiction variant"},
            {"pattern": r"sus(?:\s*(?:veh|auto))?|\bsusp\b|suspicious(?:\s*activity)?", "note": "Suspicious variants"},
            {"pattern": r"\bharassment\b",                              "note": ""},
            {"pattern": r"drive by",                                   "note": ""},
            {"pattern": r"leaving the scene",                           "note": ""},
            {"pattern": r"\btstop\b",                                   "note": ""},
            {"pattern": r"\bracing\b",                                  "note": ""},
            {"pattern": r"\bskimmer\b",                                 "note": ""},
            {"pattern": r"\bdice\b",                                    "note": ""},
            {"pattern": r"\bresisting\b",                               "note": ""},
            {"pattern": r"\bbrandishing\b",                              "note": ""},
            {"pattern": r"\bvehtft\b",                                   "note": ""},
            {"pattern": r"person shot",                                   "note": ""},
            {"pattern": r"hit / run",                                    "note": ""},
            {"pattern": r"\bindecent\b",                                 "note": ""},
            {"pattern": r"\braid\b",                                   "note": ""},
            {"pattern": r"officer safety",                                   "note": ""},
            {"pattern": r"\bcollision\b",                                   "note": ""},
            {"pattern": r"\bsubpoena\b",                                   "note": ""},
            {"pattern": r"\baslt\b",                                   "note": ""},
            {"pattern": r"\bhostage\b",                                   "note": ""},
            {"pattern": r"breaking and entering",                                   "note": ""},
            {"pattern": r"family violence",                                   "note": ""},
            {"pattern": r"property damage",                                   "note": ""},
            {"pattern": r"\bspeeding\b",                                   "note": ""},
            {"pattern": r"\bintimidation\b",                                   "note": ""},
            {"pattern": r"\bfight\b",                                   "note": ""},
            {"pattern": r"\bthft\b",                                   "note": ""},
            {"pattern": r"\bstlen\b",                                   "note": ""},
            {"pattern": r"\bburlary\b",                                   "note": ""},
            {"pattern": r"damage to prop",                                   "note": ""},
            {"pattern": r"disorderly conduct",                                   "note": ""},
            {"pattern": r"agg bat",                                   "note": ""},
            {"pattern": r"missing person",                                   "note": ""},
            {"pattern": r"stollen veh",                                   "note": ""},
            {"pattern": r"\bdementia\b",                                   "note": ""},
            {"pattern": r"\bsilver\b",                                   "note": ""},
            {"pattern": r"\bhit/skip\b",                                   "note": ""},
            {"pattern": r"agg ass",                                   "note": ""},
            {"pattern": r"\bspeed\b",                                   "note": ""},
            {"pattern": r"\bintox\b",                                   "note": ""},
            {"pattern": r"\btint\b",                                   "note": ""},
            {"pattern": r"\bskimmers\b",                                   "note": ""},
            {"pattern": r"\bfugitive\b",                                   "note": ""},
            {"pattern": r"\bfugtive\b",                                   "note": ""},
            {"pattern": r"\bhoicide\b",                                   "note": ""},
            {"pattern": r"\bcocaine\b",                                   "note": ""},
            {"pattern": r"\bcontraband\b",                                   "note": ""},
            {"pattern": r"\baudit\b",                                   "note": ""},
            {"pattern": r"\bpredator\b",                                   "note": ""},
            {"pattern": r"\bdriveby\b",                                   "note": ""},
            {"pattern": r"\bamber\b",                                   "note": ""},
            {"pattern": r"\bhitrun\b",                                   "note": ""},
            {"pattern": r"\bsex off\b",                                   "note": ""},
            {"pattern": r"animal cruelty",                                   "note": ""},
            {"pattern": r"\bhit+and+run\b",                                   "note": ""},
            {"pattern": r"no insurance",                                   "note": ""},
            {"pattern": r"rifle shipment",                                   "note": ""},
            {"pattern": r"child exploitation",                                   "note": ""},
            {"pattern": r"deadly conduct",                                   "note": ""},
            {"pattern": r"\bawim\b",                                   "note":"assult with intent to murder"},
            {"pattern": r"10[-\s][0-9][0-9]|\b10\d{2}\b",              "note": "pattern match police codes: ex. 10-99, 10 56"},
            {"pattern": r"deadly conduct",                                   "note": ""},
            {"pattern": r"cat converter",                                   "note": ""},
            {"pattern": r"sexual offender",                                   "note": ""},
            {"pattern": r"driving complaint",                                   "note": ""},
            {"pattern": r"highway violence",                                   "note": ""},
            {"pattern": r"atm jackpotting",                                   "note": ""},
            {"pattern": r"dom batt",                                   "note": ""},
            {"pattern": r"agg+rob",                                   "note": ""},
        ],

        "ambiguous": [
            {"pattern": r"daytime search for best result",             "note": "no context - seems like a trend across many agencies"},
            {"pattern": r"\bsuspect\b",                                "note": "Suspect — no crime context"},
            {"pattern": r"\bretail\b",                                 "note": "Retail — location not reason"},
            {"pattern": r"\bviper\b",                                  "note": "VIPER — unit name not reason"},
            {"pattern": r"follow\s*up",                                "note": "Follow-up — no crime context"},
            {"pattern": r"public\s*safety",                            "note": "Public safety — too broad"},
            {"pattern": r"reckless",                                   "note": "Reckless — no charge context"},
            {"pattern": r"\bmyoc\b|\bcsa\b|\bela\b|\bispern\b|\bcdtp\b|\boop\b|\bdoc\b|\bddsi\b|\brtcc\b|\bcpat\b", "note": "Unclear acronyms"},
            {"pattern": r"\bpolice\b|\bcrime[s]?\b|\bfelony\b|\bcriminal(?:\s*justice(?:\s*purpose)?)?\b|\bcrim\b", "note": "Too generic — no crime type"}, 
            {"pattern": r"inv(?:e?s?t(?:i?g(?:at(?:ion|e|ive|or)?|i(?:on)?)?)?)?|\binves\b|\binvst\b|\binvest\b|\bcriminv\b|invetigation|investgation|invet|inspectigation|investigaton", "note": "Investigation abbreviations — no crime type"},
            {"pattern": r"\bvehicle\b|\bplate\b|\btags?\b|\blprs?\b|\blookup\b|\blprr\b|\bflock\b", "note": "Describes tool/object/vendor, not reason"},  
            {"pattern": r"\b(?:na|other|work|viper|assist|reads|follow\s*up|travel(?:\s*check)?|public\s*safety|elude|evad(?:e|ing)|tag)\b", "note": "Vague operational terms"},
            {"pattern": r"working|research|fled|suspended|hit\s*skip|offense|target|suspects?\s*vehicle|\bsubject\b", "note": "Vague activity terms"},  
            {"pattern": r"buckeye",                                    "note": "Buckeye"},
            {"pattern": r"\bassist\b",                                 "note": "Assist"},
            {"pattern": r"travel\s*(?:check)?",                        "note": "Travel check"},
            {"pattern": r"\b(?:work|other|na|pd|le|lpr|lprs|leo|ci|cj|tag|lookup)\b", "note": "Generic operational references"},
            {"pattern": r"reasonable\s*suspicion",                     "note": "Vague standard — no crime type"},
            {"pattern": r"\bdoa\b",                                    "note": "Dead on arrival — ALPR reason unclear"},
            {"pattern": r"\bpatrol\b",                                 "note": "On patrol — proactive use"},
            {"pattern": r"\bsearch\b|\bquery\b|\binquiry\b|\bevidence\b", "note": "Describes action/artifact, not reason"},  
            {"pattern": r"\bdrugs?\b",                                 "note": "Drugs — no specifics"},
            {"pattern": r"^\s*in\s*$",                                 "note": "Meaningless entry"},
            {"pattern": r"law\s*enforcement|\bleo\b|\ble\b|\bpd\b|\bci\b|\bcj\b", "note": "Generic LE references — not a reason"},
            {"pattern": r"\blocate\b|\bwant\b|\binterest\b",           "note": "No subject or reason specified"}, 
            {"pattern": r"\bcase\b",                                   "note": "Case present but no number or type"},
            {"pattern": r"intel{1,2}",                                 "note": "Intelligence — no specifics"},
            {"pattern": r"\binfo\b",                                   "note": "Info — not a reason"},
            {"pattern": r"criminal\s*justice(?:\s*purpose)?",          "note": "Circular — not a reason"},
            {"pattern": r"probable\s*cause",                           "note": "Standard stated, no crime type"},
            {"pattern": r"eluding|evading",                            "note": "Without crime context"},
            {"pattern": r"\busms\b",                                   "note": "USMS — United States Marshals Service"},
            {"pattern": r"\bhidta\b",                                  "note": "HIDTA — High Intensity Drug Trafficking Area"},
            {"pattern": r"\bciu\b",                                    "note": "CIU — Criminal Investigations Unit"},
            {"pattern": r"\bcgic\b",                                   "note": "CGIC — Crime Gun Intelligence Center"},
            {"pattern": r"\bfelon\b",                                  "note": "Felon — convicted felon subject"},
            {"pattern": r"\bmisc\b",                                   "note": "Miscellaneous — no specifics"},
            {"pattern": r"\bcheck\b",                                  "note": "Check — action not reason"},
            {"pattern": r"\bstop\b",                                   "note": "Stop — no context"},
            {"pattern": r"\bfile\s*1\b",                               "note": "File 1 — administrative, no crime type"},
            {"pattern": r"\binformation\b",                            "note": "Information — no specifics"},
            {"pattern": r"\bbulletin\b",                               "note": "Bulletin — no crime context"},
            {"pattern": r"\btraf\b",                                   "note": "Traf — too abbreviated without context"},
            {"pattern": r"\binter\b",                                  "note": "Inter — too abbreviated without context"},
            {"pattern": r"\bshpd\b",                                   "note": "SHPD — agency abbreviation, not a reason"},
            {"pattern": r"\bwitness\b",                                "note": "Witness — no crime context"},
            {"pattern": r"\bprison\b",                                 "note": "Prison — no crime context"},
            {"pattern": r"\bfirearms?\b",                              "note": "Firearms — no crime context"},
            {"pattern": r"\bstoln\b|\bstoen\b",                        "note": "Stoln/stoen — stolen typo variants"},
            {"pattern": r"task\s*force",                               "note": "Task force — unit name not reason"},
            {"pattern": r"in\s*progress",                              "note": "In progress — no crime specified"},
            {"pattern": r"\blocation\b",                               "note": "Location — no crime context"},
            {"pattern": r"\bfederal\b",                                "note": "Federal — jurisdiction not reason"},
            {"pattern": r"\bdispatch\b",                               "note": ""},
            {"pattern": r"\bcall\b",                                   "note": ""},
            {"pattern": r"\bmiss\b",                                   "note": ""},
            {"pattern": r"\bimproper\b",                               "note": ""},
            {"pattern": r"\barrest\b",                                 "note": ""},
            {"pattern": r"\bhits\b",                                   "note": ""},
            {"pattern": r"\btrends\b",                                 "note": ""},
            {"pattern": r"\bcivil\b",                                  "note": ""},
            {"pattern": r"\binestigation\b",                           "note": ""},
            {"pattern": r"\bimvestigation\b",                          "note": ""},
            {"pattern": r"\bivest\b",                          "note": ""},
            {"pattern": r"\bivestigation\b",                          "note": ""},
            {"pattern": r"\benforcement\b",                            "note": ""},
            {"pattern": r"\bexpired\b",                                "note": ""},
            {"pattern": r"\bhistory\b",                                "note": ""},
            {"pattern": r"\bactive\b",                                 "note": ""},
            {"pattern": r"\bincident\b",                               "note": ""},
            {"pattern": r"\btimes\b",                                  "note": ""},
            {"pattern": r"\blost\b",                                   "note": ""},
            {"pattern": r"\bidentify\b",                               "note": ""},
            {"pattern": r"\bunauthorized\b",                           "note": ""},
            {"pattern": r"\btrip\b",                                   "note":""},
            {"pattern": r"\bsled\b",                                   "note":""},
            {"pattern": r"\bshield\b",                                 "note":""},
            {"pattern": r"\wnted\b",                                   "note":""},
            {"pattern": r"fbi request",                                "note":""},
            {"pattern": r"public health",                              "note":""},
            {"pattern": r"pointing & presenting",                      "note":""},
            {"pattern": r"look up",                                    "note":""},
            {"pattern": r"officer request",                            "note":""},
            {"pattern": r"\btrip\b",                                   "note":""},
            {"pattern": r"\bfollow-up\b",                              "note":""},
            {"pattern": r"\bcharges\b",                                "note":""},
            {"pattern": r"\bmedical\b",                                "note":""},
            {"pattern": r"\btrend\b",                                  "note":""},
            {"pattern": r"\bwanyed\b",                                 "note":""},
            {"pattern": r"\bplates\b",                                 "note":""},
            {"pattern": r"\bauto\b",                                   "note":""},
            {"pattern": r"\bbeverage\b",                               "note":""},
            {"pattern": r"\bundefined\b",                              "note":""},
            {"pattern": r"crimal justice",                             "note":""},
            {"pattern": r"\brtic\b",                                   "note":"real time crime center is not specific "},
            {"pattern": r"\bnortaf\b",                                   "note":"northern regional task force is not specific "},
            {"pattern": r"\bvcat\b",                                   "note":"violent crime apprehension team? - not specific "},
            {"pattern": r"\bsatg\b",                                   "note":"sexual assault task group? - not specific "},
            {"pattern": r"\bcrimpat\b",                                   "note":""},
            {"pattern": r"\bvsca\b",                                   "note":""},
            {"pattern": r"\bcmatt\b",                                   "note":""},
            {"pattern": r"\bfsra\b",                                   "note":""},
            {"pattern": r"\bcphiu\b",                                   "note":""},
            {"pattern": r"\bveht\b",                                   "note":""},
            {"pattern": r"\bdvopv\b",                                   "note":""},
            {"pattern": r"\bfsgi\b",                                   "note":""},
            {"pattern": r"\bwopc\b",                                   "note":""},
            {"pattern": r"\budda\b",                                   "note":""},
            {"pattern": r"\b5ddd\b",                                   "note":""},
            {"pattern": r"\bdhetf\b",                                   "note":""},
        ],

        "redacted": [
            {"pattern": r"\bredacted\b",                                   "note":""},
        ]
    }

    # ---------------------------------------------------------------------------
    # Case number pattern
    # ---------------------------------------------------------------------------
    # Matches known agency-specific case number prefixes followed by digits.
    # Used as a fallback category when the reason field contains a case number
    # but no crime-type keyword. year-prefixed formats (24xxxx, 25xxxx, 26xxxx).
    CASE_NUMBER_PATTERN = r"(whp|csp|gep|sop|lop|dgp|dp|wrp|whpc|bcso|ny|so|mv|sw|rgv|s|gepc|cspc|sopc|lopc|pg|k|sg|ij|jv|fhp|p|e|nnc|pd|r|c|ppd|\b2[456][\w\-]{3,})\d+|nnc\s\d+|202[45][\s\d-]+"

    # ---------------------------------------------------------------------------
    # Pattern of life override — applied AFTER initial categorization
    # ---------------------------------------------------------------------------
    # "pattern of life" alone → concerning
    # "pattern of life" + a crime term → reclassify to legitimate
    # This is done in post-processing because the regex engine (Rust) does not
    # support lookaheads, and the crime term can appear before OR after the phrase.
    # ---------------------------------------------------------------------------
    _POL_RE = re.compile(r"pattern\s*of\s*life", re.IGNORECASE)
    _POL_CRIME_RE = re.compile(
        r"csam|homici?de|homiocide|homcide|murder|robbery|vandalism|destruction|"
        r"damage|arson|assault|burglary|theft|trafficking|fraud|narco|drug|"
        r"shooting|stabbing|kidnap|abduction|sex\s*(?:assault|offend)|rape|investigat",
        re.IGNORECASE
    )

    def _reclassify_pattern_of_life(row: dict) -> str:
        """
        If the reason contains 'pattern of life' AND a crime keyword,
        override the category to 'legitimate'. Otherwise keep as-is.
        """
        cat = row["reason_category"]
        reason = row["reason"] or ""
        if cat == "concerning" and _POL_RE.search(reason):
            if _POL_CRIME_RE.search(reason):
                return "legitimate"
        return cat

    # ---------------------------------------------------------------------------
    # Compile per-category regex strings
    # ---------------------------------------------------------------------------
    def _build_regex(group: list[dict]) -> str:
        """Join all patterns in a group into a single alternation regex."""
        return "|".join(f"(?:{entry['pattern']})" for entry in group)

    _compiled: dict[str, str] = {
        cat: _build_regex(entries)
        for cat, entries in PATTERN_GROUPS.items()
    }

    # ---------------------------------------------------------------------------
    # Categorization function
    # ---------------------------------------------------------------------------
    def categorize_reasons(input_lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Priority order (first match wins):
          1. test          — contains the word 'test'; likely training/system access, safe to set first to cover edge cases 
          2. concerning    — policy-sensitive; First Amendment issues, non-investigative use
          3. legitimate    — named crime type or recognized law enforcement activity
          4. case_number   — reason field contains an agency case number pattern
          5. ambiguous     — too vague to classify but not inherently improper
          6. null          — reason field is entirely absent
          7. numeric_code  — reason is entirely numeric; likely an internal code
          8. ambiguous     — short reason (<4 chars) that matched no other category
          9. uncategorized — non-null reason that matched no pattern (taxonomy gap)
        """
        _cat_lf = input_lf.with_columns([
            pl.when(pl.col("reason").str.contains(r"(?i).*\btest\b.*|.*\btesting\b/*"))
                .then(pl.lit("test"))
              .when(pl.col("reason").str.contains(f"(?i){_compiled['concerning']}") & pl.col('case_number').is_null()) # will only now be concerning if there isn't a case number attached. This will hopefully reduce the count of false positives for the concerning category due to it being priority over the legitimate category 
                .then(pl.lit("concerning"))
              .when(pl.col("reason").str.contains(f"(?i){_compiled['legitimate']}"))
                .then(pl.lit("legitimate"))
              .when(pl.col("reason").str.contains(f"(?i){CASE_NUMBER_PATTERN}"))
                .then(pl.lit("case_number"))
              .when(pl.col("reason").str.contains(f"(?i){_compiled['ambiguous']}")) # will not filter by with/without case number since the governance issue being analyzed is not dependent on case number present. 
                .then(pl.lit("ambiguous"))
              .when(pl.col("reason").is_null())
                .then(pl.lit("null"))
              .when(pl.col("reason").str.contains(r"^\d+$"))
                .then(pl.lit("numeric_code"))
              .when(pl.col("reason").str.len_chars() < 4)
                .then(pl.lit("ambiguous"))
              .when(pl.col("reason").str.contains(f"(?i){_compiled['redacted']}"))
                .then(pl.lit("redacted"))
              .otherwise(pl.lit("uncategorized"))
              .alias("reason_category")
        ])
        # pattern of life override — map_elements requires a collected DataFrame;
        # collect, apply UDF, then re-lazify so callers get a LazyFrame back.
        _collected = _cat_lf.collect()
        _overridden = _collected.with_columns(
            pl.struct(["reason", "reason_category"])
              .map_elements(_reclassify_pattern_of_life, return_dtype=pl.String)
              .alias("reason_category"))
        return _overridden.lazy()

    cat_lf = categorize_reasons(lf)
    # Collect once for cells that need a DataFrame (height, map_elements, etc.)
    cat_df = cat_lf.collect()
    return CASE_NUMBER_PATTERN, cat_lf


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Full Category Report

    The report below shows:
    1. The category distribution table (count, % of total, missing case # count per category)
    2. The top 15 most frequent reason strings within each category (excluding null,
       numeric_code, and case_number categories, which are self-explanatory by definition)

    This output is the primary reference for validating the taxonomy. Analysts should
    scan each category's top reasons to confirm correct classification and identify
    patterns that should be moved between categories.
    """)
    return


@app.cell
def _(TOTAL_SEARCHES, cat_lf, pl):
    # ---------------------------------------------------------------------------
    # Reporting utilities
    # ---------------------------------------------------------------------------

    def display_category(cat_lf: pl.LazyFrame, category: str) -> None:
        """Print all raw rows belonging to a single category for full audit."""
        result = cat_lf.filter(pl.col("reason_category") == category).collect()
        print(f"\n{'='*60}")
        print(f"ALL ROWS — {category.upper()}  ({len(result):,} rows)")
        print("=" * 60)
        print(result)

    def get_distribution(input_lf: pl.LazyFrame) -> pl.DataFrame:
        """
        Returns a summary table of category counts, missing case # counts, and
        percentage of total searches. Denominator is always the full dataset height
        (TOTAL_SEARCHES) for consistency with other percentage calculations.
        """
        return (
            input_lf.group_by("reason_category")
            .agg([
                pl.col("search time").count().alias("count"),
                pl.col("case_number").is_null().sum().alias("missing_case_num"),
            ])
            .with_columns(
                (pl.col("count") / TOTAL_SEARCHES * 100).round(1).alias("percent_of_total")
            )
            .sort("count", descending=True)
            .collect()
        )

    def sample_reasons(cat_lf: pl.LazyFrame, n: int = 15, category: str | None = None) -> None:
        """
        Print the top N most frequent reason strings for each category.
        Skips null, numeric_code, and case_number since their reason values are
        either absent or self-evident and add no analytical value when listed.
        """
        skip_cats = {"null", "case_number", "redacted"}
        if category:
            cats = [category]
        else:
            cats = (
                cat_lf.select(pl.col("reason_category").drop_nulls().unique().sort())
                .collect()
                .to_series()
                .to_list()
            )

        for cat in cats:
            if cat in skip_cats:
                continue
            top = (
                cat_lf
                .filter(pl.col("reason_category") == cat)
                .group_by("reason")
                .agg(pl.len().alias("count"))
                .sort("count", descending=True)
                .collect()
                .head(n)
            )
            print(f"\n{'='*60}")
            print(f"TOP {n} REASONS — {cat.upper()}")
            print("=" * 60)
            print(top)


    def full_report(cat_lf: pl.LazyFrame, sample_n: int = 15) -> None:
        """
        Print the category distribution summary followed by sampled reasons
        for every non-trivial category.
        """
        print("\n" + "=" * 60)
        print("CATEGORY DISTRIBUTION")
        print("=" * 60)
        print(get_distribution(cat_lf))
        sample_reasons(cat_lf, n=sample_n)

    full_report(cat_lf)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Uncategorized Reason Audit

    Records falling through to `uncategorized` represent gaps in the taxonomy — they contain
    a non-null, non-trivial reason string that did not match any defined pattern. These should
    be reviewed after each new data batch to determine whether new patterns should be added.

    It can be observed that there are many mispelled reaons which fall into already defined
    categories however the current percent of categorized reasons is sufficient for analysis.
    """)
    return


@app.cell
def _(cat_lf, pl):
    # ---------------------------------------------------------------------------
    # Audit uncategorized records — these represent taxonomy gaps.
    # ---------------------------------------------------------------------------
    _uncategorized = (
        cat_lf.filter(pl.col("reason_category") == "uncategorized")
        .group_by("reason")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .collect()
    )
    _total_uncategorized = (
        cat_lf.filter(pl.col("reason_category") == "uncategorized")
        .select(pl.len())
        .collect()
        .item()
    )
    print(f"Uncategorized reason strings: {_uncategorized.height:,} unique values")
    print(f"Total uncategorized records:  {_total_uncategorized:,}")
    print("\nAll uncategorized reason strings (sorted by frequency):")
    print(_uncategorized.head(50))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Prevalence of Vague Access Reasons
    """)
    return


@app.cell
def _(TOTAL_SEARCHES, cat_lf, pl):
    # ---------------------------------------------------------------------------
    # Vague access reason prevalence
    # ---------------------------------------------------------------------------

    vague_df = (
        cat_lf.group_by("reason_category")
        .agg([
            pl.len().alias("searches"),
            (
                pl.col("case_number").is_null()
                | (pl.col("case_number").str.strip_chars() == "")
            ).sum().alias("missing_case_num"),
        ])
        .with_columns([
            (pl.col("searches") / TOTAL_SEARCHES * 100).round(2).alias("pct_of_all_searches"),
            (pl.col("missing_case_num") / pl.col("searches") * 100).round(2).alias("pct_missing_case_num"),
        ])
        .sort("searches", descending=True)
        .collect()
    )

    # Append a totals row
    totals = pl.DataFrame({
        "reason_category":     ["TOTAL"],
        "searches":            [vague_df["searches"].sum()],
        "missing_case_num":    [vague_df["missing_case_num"].sum()],
        "pct_of_all_searches": [round(vague_df["searches"].sum() / TOTAL_SEARCHES * 100, 2)],
        "pct_missing_case_num":[round(vague_df["missing_case_num"].sum() / vague_df["searches"].sum() * 100, 2)],
    }).cast({
        "searches":         pl.UInt32,
        "missing_case_num": pl.UInt32,
    })

    print("\nVague Access Reason Prevalence")
    print("=" * 60)
    print(pl.concat([vague_df, totals]))
    return (vague_df,)


@app.cell
def _(pl, plt, vague_df):
    import matplotlib.ticker as mticker
    import numpy as np

    # ---------------------------------------------------------------------------
    # Prep — exclude the TOTALS row and derive "has case number" count
    # ---------------------------------------------------------------------------
    plot_df = vague_df.with_columns(
        (pl.col("searches") - pl.col("missing_case_num")).alias("has_case_num")
    ).sort("searches", descending=True)

    categories = plot_df["reason_category"].to_list()
    searches   = plot_df["searches"].to_list()
    has_case   = plot_df["has_case_num"].to_list()

    x     = np.arange(len(categories))
    width = 0.45

    # ---------------------------------------------------------------------------
    # Plot
    # ---------------------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(max(10, len(categories) * 1.3), 6))

    bars_total = ax.bar(x - width / 2, searches, width, label="Total Searches",    color="#4C72B0", zorder=3)
    bars_case  = ax.bar(x + width / 2, has_case,  width, label="With Case Number", color="#55A868", zorder=3)

    # Value labels on top of every bar
    for bar in bars_total:
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(searches) * 0.01,
            f"{int(bar.get_height()):,}",
            ha="center", va="bottom", fontsize=8, color="black", fontweight="bold",
        )
    for bar in bars_case:
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(searches) * 0.01,
            f"{int(bar.get_height()):,}",
            ha="center", va="bottom", fontsize=8, color="black", fontweight="bold",
        )

    # Axes formatting
    ax.set_xticks(x)
    ax.set_xticklabels(categories, rotation=35, ha="right", fontsize=9, color="black")
    ax.tick_params(axis="both", colors="black")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.set_ylabel("Number of Searches", fontsize=11, color="black")
    ax.set_title(
        "Distribution of Searches by Reason Category\nwith Case Number Attachment Rate",
        fontsize=13, fontweight="bold", color="black",
    )
    ax.legend(fontsize=10, labelcolor="black", framealpha=1, edgecolor="#cccccc")
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0, color="#aaaaaa")
    ax.set_axisbelow(True)
    legend = ax.legend(fontsize=10, labelcolor="black", framealpha=1, edgecolor="#cccccc", facecolor="white")

    # Spine colours
    for spine in ax.spines.values():
        spine.set_edgecolor("black")

    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    plt.tight_layout()
    plt.savefig("search_distribution_by_reason.png", dpi=150, bbox_inches="tight", facecolor="white")
    plt.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Case Number Coverage — Detailed Analysis

    This section refines the case number analysis to apply a more precise definition of
    "missing case number."

    A search is counted as **missing a case number** only when **both** of the following
    are true:
    - The `case_number` column is null or blank, **AND**
    - The `reason` field does not contain a recognizable case number pattern

    A search has an identifiable case reference if either the dedicated column is populated
    **or** the officer embedded a case number in the reason text.
    """)
    return


@app.cell
def _(CASE_NUMBER_PATTERN, TOTAL_SEARCHES, is_case_missing, lf, pl):
    # A record has a case reference if the case # column is populated OR
    # the reason text contains a case number pattern.
    has_case_ref = (
        ~is_case_missing()
        | pl.col("reason").str.contains(f"(?i){CASE_NUMBER_PATTERN}")
    )

    null_case_count = lf.filter(~has_case_ref).select(pl.len()).collect().item()
    null_case_pct = (null_case_count / TOTAL_SEARCHES) * 100

    print("Case Number Coverage")
    print("=" * 60)
    print(f"  Total searches:     {TOTAL_SEARCHES:,}")
    print(f"  Searches WITH case reference:  {TOTAL_SEARCHES - null_case_count:,}")
    print(f"  Searches WITHOUT case ref:     {null_case_count:,}  ({null_case_pct:.1f}%)")
    return (null_case_count,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Completely Undocumented Searches

    A search is considered **completely undocumented** when all three of the following
    fields are simultaneously absent:
    - `case #` — no case number
    - `reason` — no stated reason
    - `text prompt` — no descriptive vehicle search

    These records represent the most significant governance compliance concern: an officer
    accessed the surveillance system and left no record of why.
    """)
    return


@app.cell
def _(TOTAL_SEARCHES, cat_lf, pl):
    # ---------------------------------------------------------------------------
    # Identify searches where ALL three justification fields are absent.
    # ---------------------------------------------------------------------------
    _count = (
        cat_lf.filter(
            (pl.col("case_number").is_null() | (pl.col("case_number").str.strip_chars() == ""))
            & (pl.col("reason").is_null() | (pl.col("reason").str.strip_chars() == ""))
            & (pl.col("text prompt").is_null() | (pl.col("text prompt").str.strip_chars() == ""))
        )
        .select(pl.len())
        .collect()
        .item()
    )
    _pct = (_count / TOTAL_SEARCHES) * 100

    print("Completely Undocumented Searches (no case #, no reason, no text prompt)")
    print("=" * 60)
    print(f"  Count:  {_count:,}")
    print(f"  Total:  {TOTAL_SEARCHES:,}")
    print(f"  Rate:   {_pct:.3f}%")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Concerning Searches Without a Case Number

    This is a compound compliance measure: searches that were classified as `concerning`
    (suggesting policy-sensitive or non-investigative access) **and** also lack a case
    number. These are the records most likely to represent misuse or policy non-compliance
    and should be the primary focus of any follow-up records request or audit.

    Note that `concerning` classification is based on researcher-defined patterns and is
    not a legal determination. The absence of a case number does not by itself establish
    a policy violation — some agency policies permit certain search types (e.g., proactive
    patrol) without requiring a case number.
    """)
    return


@app.cell
def _(TOTAL_SEARCHES, cat_lf, pl):
    # ---------------------------------------------------------------------------
    # Concerning searches that also lack a case number.
    # These represent the highest-priority records for manual review.
    # ---------------------------------------------------------------------------
    _concerning_no_case_count = (
        cat_lf.filter(
            (pl.col("reason_category") == "concerning")
            & (pl.col("case_number").is_null() | (pl.col("case_number").str.strip_chars() == ""))
        )
        .select(pl.len())
        .collect()
        .item()
    )
    _concerning_count = (
        cat_lf.filter(pl.col("reason_category") == "concerning")
        .select(pl.len())
        .collect()
        .item()
    )
    _pct_of_total = round(_concerning_no_case_count / TOTAL_SEARCHES * 100, 4)
    _pct_of_concerning = round(_concerning_no_case_count / _concerning_count * 100, 2)

    print("Concerning Searches Without a Case Number")
    print("=" * 60)
    print(f"  Count:               {_concerning_no_case_count:,}")
    print(f"  % of all searches:   {_pct_of_total}%")
    print(f"  % of all concerning: {_pct_of_concerning}%")
    print(f"\nTop reason strings in this group (by frequency):")
    print(
        cat_lf.filter(pl.col("reason_category") == "concerning")
        .group_by("reason")
        .agg([
            pl.len().alias("total_count"),
            (
                pl.col("case_number").is_not_null()
                & (pl.col("case_number").str.strip_chars() != "")
            ).sum().alias("with_case_number"),
        ])
        .sort("total_count", descending=True)
        .collect()
        .head(100)
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Confidence Intervals for Key Proportions

    Utilize Wilson score to determine confidence interval for proportions.

    The formula for the Wilson score 95% CI is:

    $$\hat{p}_{Wilson} = \frac{\hat{p} + \frac{z^2}{2n} \pm z\sqrt{\frac{\hat{p}(1-\hat{p})}{n} + \frac{z^2}{4n^2}}}{1 + \frac{z^2}{n}}$$

    where $z = 1.96$ for a 95% confidence level.
    """)
    return


@app.cell
def _(
    TOTAL_SEARCHES,
    cat_lf,
    null_case_count,
    pl,
    proportion_confint,
    total_wheaton_searches,
    wheaton_without_case_count,
):
    # ---------------------------------------------------------------------------
    # Wilson score 95% confidence intervals for key proportions.
    # ---------------------------------------------------------------------------

    def wilson_ci(successes: int, n: int) -> tuple[float, float]:
        """
        Compute the Wilson score 95% confidence interval for a proportion.
        Args:
            successes: number of events of interest
            n:         total number of trials
        Returns:
            (lower, upper) bounds as percentages
        """
        if n == 0:
            return (0.0, 100.0)
        low, high = proportion_confint(count=successes, nobs=n, alpha=0.05, method='wilson')
        return (round(low * 100, 2), round(high * 100, 2))

    # Compute counts for key proportions using lazy evaluation
    def _count_lf(expr: pl.Expr) -> int:
        return cat_lf.filter(expr).select(pl.len()).collect().item()

    n_concerning = _count_lf(pl.col("reason_category") == "concerning")
    n_concerning_no_case = _count_lf(
        (pl.col("reason_category") == "concerning")
        & (pl.col("case_number").is_null() | (pl.col("case_number").str.strip_chars() == ""))
    )
    n_completely_undoc = _count_lf(
        (pl.col("case_number").is_null() | (pl.col("case_number").str.strip_chars() == ""))
        & (pl.col("reason").is_null() | (pl.col("reason").str.strip_chars() == ""))
        & (pl.col("text prompt").is_null() | (pl.col("text prompt").str.strip_chars() == ""))
    )

    metrics = [
        ("All agencies — searches without any case reference",
         null_case_count, TOTAL_SEARCHES),
        ("Wheaton PD   — searches without case # in column",
         wheaton_without_case_count, total_wheaton_searches),
        ("All agencies — concerning category",
         n_concerning, TOTAL_SEARCHES),
        ("All agencies — concerning AND no case #",
         n_concerning_no_case, TOTAL_SEARCHES),
        ("All agencies — completely undocumented",
         n_completely_undoc, TOTAL_SEARCHES),
    ]

    results = []
    for label, successes, n in metrics:
        est_pct = round(successes / n * 100, 2) if n > 0 else 0.0
        lo, hi = wilson_ci(successes, n)
        results.append({
            "Metric": label,
            "n": successes,
            "N": n,
            "Est %": est_pct,
            "95% CI": f"[{lo:.2f}%, {hi:.2f}%]"
        })

    results_df = pl.DataFrame(results)
    print("Key Proportions with 95% Wilson Score Confidence Intervals")
    print(results_df)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
