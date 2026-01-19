import marimo

__generated_with = "0.19.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl 
    import matplotlib as plt 
    import numpy as np 
    from pathlib import Path 
    return Path, mo, pl


@app.cell
def _(pl):
    # polars configs 
    pl.Config.set_tbl_rows(-1)  # Show all rows
    pl.Config.set_tbl_cols(-1)  # Show all columns 
    return


@app.cell
def _(Path):
    access_logs_path = Path("data/access_logs/")
    access_logs_files_list = [file for file in access_logs_path.rglob("*") if (file.is_file()) & (file.suffix == '.csv')]
    for _ in access_logs_files_list:
        print(_)
    return (access_logs_files_list,)


@app.cell
def _(access_logs_files_list, pl):
    dfs = [pl.read_csv(file) for file in access_logs_files_list]

    # Standardize column names: strip whitespace and convert to lowercase
    standardized_dfs = []
    for _df in dfs:
        # Create a mapping of old column names to cleaned names
        _df = _df.rename({col: col.strip().lower() for col in _df.columns})
        standardized_dfs.append(_df)

    # make combined dataframe 
    df = pl.concat(standardized_dfs, how="diagonal")

    # parse date times 

    # first, do the time frame for the investigator search window 
    # Split the time frame into start and end dates
    df = df.with_columns([
        pl.col('time frame').str.split('\n').list.get(0).alias('start_time_of_search_window'),
        pl.col('time frame').str.split('\n').list.get(1).alias('end_time_of_search_window')
    ])

    # Parse both to datetime
    df = df.with_columns([
        pl.col('start_time_of_search_window').str.strptime(pl.Datetime, format='%m/%d/%Y, %I:%M:%S %p %Z'),
        pl.col('end_time_of_search_window').str.strptime(pl.Datetime, format='%m/%d/%Y, %I:%M:%S %p %Z')
    ])

    # Calculate duration of time frame 
    df = df.with_columns(
        (pl.col('end_time_of_search_window') - pl.col('start_time_of_search_window')).alias('search_window_duration')
    )

    # second, parse the search time (time when officer initiated their search in the Flock system)
    df = df.with_columns([
        pl.col('search time').str.strptime(
            pl.Datetime, 
            format='%m/%d/%Y, %I:%M:%S %p %Z'
        ).alias('search_datetime')
    ]).with_columns([
        # Classify time of day
        pl.when(pl.col('search_datetime').dt.hour().is_between(6, 17))
          .then(pl.lit('business_hours'))
          .when(pl.col('search_datetime').dt.hour().is_between(18, 22))
          .then(pl.lit('evening'))
          .otherwise(pl.lit('night'))
          .alias('time_period_when_officer_initiated_search')
    ])

    df.glimpse()
    return (df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Analysis of Access Logs for Flock Safety System's by Police Department

    This markdown is merely a guideline for myself to focus my analysis on relevant subjects

    ##Tier 1: CRITICAL VALUE
    ###1. Audit Trail Integrity Analysis
    Parse the reason field and case # to determine what percentage of searches are:

    Justified by explicit case number → Auditable (case # has value in column)

    Justified by ambiguous code ("lpr hit verification", "btmv") → Questionably auditable

    Missing both case # AND clear reason → Unauditable

    Why: ISO 27001 requires demonstrable audit justification for every data access. Missing case numbers on searches = governance control failure. Quantify the exposure (e.g., "3,847 searches [22%] lack case numbers, making them unauditable").

    For CIOs: Shows what happens when departments skip access justification controls—they become legally defenseless. This further touches on the fears of organizations failing to properly access and utilize these surveillance networks.

    ###2. Search Justification Semantics
    Analyze the reason field values—what do "whp25009165", "btmv", "lpr hit verification" actually mean?

    If these are case codes, extract and categorize

    Trend over 2025: Are searches becoming more ambiguous (mission creep)?

    Why: Flock's software accepts vague reasons without enforcement—shows vendor design failure. The following FOIA data could prove this architectural gap has real consequences.

    ##Tier 2: HIGH VALUE (Operational Dysfunction & Risk)
    ###3. Role-Based Access Control (RBAC) Distribution
    Histogram: total_networks_searched across 60 officers

    Question: Do all 60 officers have access to all 1109 networks, or is access stratified?

    Flag: If every officer has 1109 networks available → no RBAC → regulatory non-compliance

    Why: Connects to Wyden letter findings (DEA officer using local detective's credentials to access national database). Your data can show if all officers have that capability.

    For CIOs: Quantify access over-provisioning risk. "60% of your force has access to national database when only 15% need it = 45 potential breach vectors."

    ###4. Temporal Pattern Analysis for Suspicious Access
    Time-of-day distribution: Are searches concentrated 9-5 (legitimate) or heavily off-hours (suspicious)?

    Per-officer: Does Officer X search during shift, Officer Y at 2 AM?

    Burst detection: Searches per hour—flag >10 searches/hour without case # as suspicious batch access

    Why: Identifies specific high-risk access patterns that should trigger investigation (connects to unauthorized access risk in your thesis).

    For CIOs: "7 officers conducted 47% of all searches during night hours with no case justification—recommend immediate audit and potential credential verification."

    ### 5. Institutional Risk Quantification
    Calculate exposure: X% of searches unauditable = X% of surveillance data legally defenseless

    Financial exposure: Multiply unauditable searches by potential liability per Santa Cruz/Denver settlements

    Access control breach risk: If one officer's compromised credential (like the DEA example), how many networks/searches exposed?

    Why: Translates technical governance failures into business risk language for your CIO audience. Moves paper from "this is bad" to "this costs $X and exposes you to Y liability."
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Officer Usage Analysis

    Analysis made:
    1. Who has done the most searches
    2. Top 15 reasons for searches
    3. Total Number of searches
    4. Percentage of searches using 'investigation' or 'inv' or similar
    5. Top 3 search reason by officer
    6. network usage by officer
    """)
    return


@app.cell
def _(df, pl):
    # who has been doing the most searches 
    search_by_person = df.group_by('name').agg(
        pl.col('search time').count().alias('# of searches')
    ).sort(by="# of searches", descending=True)

    print(search_by_person.head(15))
    # 1. Kevin Freeman 2,171 
    # 2. Brian Wagner 1,707
    # 3. Eric Sousanes 1,347
    return


@app.cell
def _(df, pl):
    # top reason 15 reasons for search
    df_all = df.group_by('reason').agg(
        pl.col('search time').count().alias('occurences')
    ).sort(by='occurences', descending=True)

    print(f'Most common reasons for searches (unfiltered): \n{df_all.head(15)}')
    return


@app.cell
def _(df, pl):
    # Total count of searches, searches with 'inv', percentage 
    total_searches = df.height 

    # Count searches where 'reason' contains 'inv' 
    inv_searches = df.filter(
        pl.col('reason').str.to_lowercase().str.contains('inv')
    ).height

    # Calculate percentage
    inv_percentage = (inv_searches / total_searches) * 100

    print(f'Total searches: {total_searches:,}')
    print(f'Searches with "inv" in reason: {inv_searches:,}')
    print(f'Percentage: {inv_percentage:.2f}%')
    return


@app.cell
def _(df, pl):
    # Get total searches per officer with top 3 reasons
    officer_analysis = df.group_by('name').agg(
        pl.col('search time').count().alias('total_searches')
    ).sort('total_searches', descending=True)

    # For each top officer, get their top 3 reasons
    top_officers = officer_analysis.head(10)['name'].to_list()

    for officer in top_officers:
        officer_reasons = df.filter(pl.col('name') == officer).group_by('reason').agg(
            pl.col('search time').count().alias('count')
        ).sort('count', descending=True).head(3)

        total = df.filter(pl.col('name') == officer).height
        print(f"\n{officer} ({total} total searches):")
        print(officer_reasons)
    return


@app.cell
def _(df, pl):
    network_usage = df.group_by('name').agg([
        pl.col('total networks searched').mean().round(1).alias('avg_networks_per_search'),
        pl.col('total networks searched').max().alias('max_networks_searched'),
        pl.col('search time').count().alias('total_searches')
    ]).sort('total_searches', descending=True)

    print(f'total, man, and avg network searches by officer: \n{network_usage.head(15)}')
    return


@app.cell
def _(mo):
    mo.md(r"""
    # Categorize Access Reasons

    Analysis made:
    1. categorize reasons, derive percentage
    2. top 20 by category
    """)
    return


@app.cell
def _(df, pl):
    # Categorize using pattern matching
    def categorize_reasons(cat_df):
        # Patterns for each category
        legitimate_patterns = '|'.join([
            'hit and run', 'theft', 'battery', 'assault', 'robbery', 'armed robbery',
            'burglary', 'stolen vehicle', 'stolen mv', 'stolen', 'dui', 'domestic',
            'domestic battery', 'warrant', 'wanted person', 'missing person', 'missing',
            'crash', 'homicide', 'shooting', 'sex assault', 'kidnapping', 'arson',
            'trespass', 'fraud', 'forgery', 'suicidal', 'sex offender', 'child abuse',
            'child porn', 'child pornography', 'traffic', 'fleeing', 'flee', 'suspicious',
            'suspect', 'amber alert', 'death inv', 'welfare check', 'check well being',
            'reckless', 'gun', 'weapons', 'retail theft', 'shoplifting', 'mvt',
            'motor vehicle theft', 'hit & run', 'h&r', 'missing juvenile', 
            'missing endangered', 'investigation', 'hnr', 'burg'
        ])

        # Case number patterns (WHPxxxxxxx, CSPxxxxxxx, etc.)
        case_patterns = r'(whp|csp|gep|sop|lop|dgp|wrp|whpc|gepc|cspc|sopc|lopc)\d+'

        ambiguous_patterns = '|'.join([
            'myoc', 'voop', 'cwb', 'csa', 'ela', 'uucc', 'btmv', 'upsmv',
            'ispern', 'mvt', 'doa', 'dwls', 'cdtp', 'uuw', 'oop', 'aoa',
            'sus veh', 'sus auto', 'sus', 'susp', 'doc', 'test', 'patrol'
        ])

        concerning_patterns = '|'.join([
            # Political/activist surveillance (1st Amendment violations)
            'political', 'activist', 'protest', 'demonstrator', 'rally',

            # Exploratory/fishing expeditions (no legitimate basis)
            'more like this',  # Suggests browsing similar cases without justification
            'daytime search for best result',  # Generic fishing
            'slack search',  # Casual/non-investigative search
            'background check',  # Without case justification = illegal surveillance

            # Potentially discriminatory profiling
            'suspicious person', # (without other context),  Can indicate profiling
            'check well being', # (when abused) Sometimes used as pretext

            # Personal/non-investigative use
            'image download',  # Why downloading images without case?
            'dispatch quick search',  # Suggests non-investigative access
            'test',  # Testing on real people's data = privacy violation
        ])

        # Categorize
        categorized = df.with_columns([
            pl.when(pl.col('reason').str.contains(f'(?i){legitimate_patterns}'))
              .then(pl.lit('legitimate'))
              .when(pl.col('reason').str.contains(f'(?i){case_patterns}'))
              .then(pl.lit('case_number'))
              .when(pl.col('reason').str.contains(f'(?i){concerning_patterns}'))
              .then(pl.lit('concerning'))
              .when(pl.col('reason').str.contains(f'(?i){ambiguous_patterns}'))
              .then(pl.lit('ambiguous'))
              .when(pl.col('reason').is_null())
              .then(pl.lit('null'))
              .when(pl.col('reason').str.len_chars() < 5)  # Very short/cryptic
              .then(pl.lit('cryptic'))
              .when(pl.col('reason').str.contains(r'^\d+$'))  # Just numbers
              .then(pl.lit('numeric_code'))
              .otherwise(pl.lit('uncategorized'))
              .alias('reason_category')
        ])

        return categorized

    # Apply categorization
    cat_df = categorize_reasons(df)

    # Get distribution
    category_distribution = cat_df.group_by('reason_category').agg([
        pl.col('search time').count().alias('count'),
        pl.col('case #').is_null().sum().alias('missing_case_num')
    ]).with_columns(
        (pl.col('count') / cat_df.height * 100).round(1).alias('percentage')
    ).sort('count', descending=True)

    print(category_distribution)
    return cat_df, category_distribution


@app.cell
def _(cat_df, category_distribution, pl):
    # Analysis of categories 

    # there are records of access WITHOUT providing a reason
    # there are a large amount of ambiguous, cryptic, and some 'concerning' reasons however these require further context. 
        # this prevents an issue though, since it means the reasons are not abundantly clear to the reading/auditor why the person is access the surveillance network 

    def display_reasons_by_category(_cat_df, reason): 
        _res = _cat_df.filter(pl.col('reason_category') == reason)
        print(_res)

    def get_unique_reasons_by_category(_cat_df):
        # Get all unique reasons per category
        for category in _cat_df['reason_category']:
            unique_reasons = _cat_df.filter(
                pl.col('reason_category') == category
            )['reason'].unique().sort()

            print(f"\n{category.upper()} ({len(unique_reasons)} unique reasons):")
            print(unique_reasons)

    def sample_reasons_from_each_category(_cat_df):
        for category in category_distribution['reason_category']:
            sample_reasons = _cat_df.filter(
                pl.col('reason_category') == category
            ).group_by('reason').agg(
                pl.col('search time').count().alias('count')
            ).sort('count', descending=True).head(20)

            print(f"\n{category.upper()} - Top 20 reasons:")
            print(sample_reasons)

    # get_unique_reasons_by_category(cat_df) # this is EVER reason 
    # display_reasons_by_category(cat_df, 'null')
    sample_reasons_from_each_category(cat_df)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Analysis of Null Case # Logs
    """)
    return


@app.cell
def _(df):
    # number and percent of searches without a case # attached 
    null_case_count = df['case #'].is_null().sum()
    null_case_pct = (null_case_count / len(df)) * 100

    print(f"Searches with NULL case #: {null_case_count} ({null_case_pct:.1f}%)")

    # This is BAD
    return


@app.cell
def _(cat_df, pl):
    # number of searches where case # and reason are empty 
    _df = cat_df.filter(
        (pl.col('case #').is_null() | (pl.col('case #').str.strip_chars() == '')) &
        (pl.col('reason').is_null() | (pl.col('reason').str.strip_chars() == ''))
    )
    _df.shape[0]
    # 7 instances of a lookup or search without case # OR reason listed 
    return


@app.cell
def _(cat_df, pl):
    # how many searches are done outside of 9-5 regular hours 
    _df = cat_df.filter(
        pl.col('time_period_when_officer_initiated_search') == 'night'
    )
    _df.shape[0] # 4895 
    return


@app.cell
def _(cat_df, pl):
    # number of searches with null case # outside of regular hours (9-5) with non-legimate appearing reason
    categories_to_search = ['concerning', 'cryptic', 'ambiguous','null']

    suspicious_searches = cat_df.filter(
        (pl.col('case #').is_null()) &
        (pl.col('time_period_when_officer_initiated_search') == 'night') &
        (pl.col('reason_category').is_in(categories_to_search))
    )

    suspicious_searches.shape[0]
    return (suspicious_searches,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Worst Offenses (potentially) Found
    below is likely not a complete or absolute list as more analysis could be done. Rows below are not for certain violations in any way but just rise from filtering
    """)
    return


@app.cell
def _(pl, suspicious_searches):
    # out of regular hours, without case #
    suspicious_searches.select(pl.exclude([
        'time frame', 'org name', 'start_time_of_search_window', 'end_time_of_search_window', 'search_date', 'search_datetime', 'search_hour', 'weekday', 'month', 'total devices searched', 'text prompt'
    ]))
    return


@app.cell
def _(cat_df, pl):
    cat_df.filter(
        (pl.col('reason_category') == 'concerning')
        &
        (pl.col('case #').is_null())
    )

    # most concerning reasons listed without a case # attached 
    # background search 
    # image download 
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
