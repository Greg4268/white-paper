import marimo

__generated_with = "0.19.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl 
    import re
    import pdfplumber
    from pathlib import Path
    return Path, pdfplumber


@app.cell
def _(Path):
    access_logs_path = Path("data/access_logs/pdf/")
    converted_pdf_write_path = Path("data/access_logs/pdf/converted_to_csv/")
    access_logs_files_list = [file for file in access_logs_path.rglob("*") if file.is_file()]
    return (access_logs_files_list,)


@app.cell
def _(access_logs_files_list, pdfplumber):
    # Extract tables from PDF
    text = ''
    for _filepath in access_logs_files_list: 
        with pdfplumber.open(_filepath) as pdf:
            print(_filepath)
            for page in pdf.pages:
                text += page.extract_text()


    lines = text.split('\n')

    print(text)

    # output_path = Path('data') / 'Rockford_IL_access_logs.csv'
    # df.write_csv(output_path)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
