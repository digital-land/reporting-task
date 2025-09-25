import pandas as pd
import argparse
import os

def endpoint_provisions_check(output_dir, include_pdf):
    # Fetch and filter Endpoint table
    endpoint_url = "https://datasette.planning.data.gov.uk/digital-land/endpoint.csv?_stream=on"
    df0 = pd.read_csv(endpoint_url)
    df0 = df0[df0['end_date'].isna()]  # Keep only active endpoints
    df_endpoint = df0[["endpoint", "end_date", "endpoint_url"]].copy()

    # Fetch and process Source table
    source_url = "https://datasette.planning.data.gov.uk/digital-land/source.csv?_stream=on"
    df1 = pd.read_csv(source_url)
    df1["organisation_ref"] = df1["organisation"].str.replace(r"^.*?:", "", regex=True).astype(str)
    df_source = df1[["endpoint", "source", "collection","organisation_ref"]].copy()

    # Fetch and filter Organisation table
    org_url = "https://datasette.planning.data.gov.uk/digital-land/organisation.csv?_stream=on"
    df2 = pd.read_csv(org_url)
    df2 = df2[df2['end_date'].isna()]
    df2["reference"] = df2["reference"].astype(str)
    df_org = df2[["name", "reference"]].copy()
    df_org.rename(columns={"name": "organisation", "reference": "organisation_ref"}, inplace=True)

    # Fetch and deduplicate Resource_endpoint table
    resource_endpoint_url = "https://datasette.planning.data.gov.uk/digital-land/resource_endpoint.csv?_stream=on"
    df3 = pd.read_csv(resource_endpoint_url)
    df_resource_endpoint = df3[["endpoint", "resource"]].drop_duplicates(subset="endpoint", keep="last")

    # Fetch and deduplicate Resource_dataset table
    resource_dataset_url = "https://datasette.planning.data.gov.uk/digital-land/resource_dataset.csv?_stream=on"
    df4 = pd.read_csv(resource_dataset_url)
    df_resource_dataset = df4[["dataset", "resource"]].drop_duplicates(subset="resource", keep="last")

    # Fetch and process Provisions table
    provisions_url = "https://datasette.planning.data.gov.uk/digital-land/provision.csv?_stream=on"
    df5 = pd.read_csv(provisions_url)
    df5["organisation"] = df5["organisation"].str.replace(r"^.*?:", "", regex=True).astype(str)
    df_provisions = df5[["dataset", "organisation"]].copy()
    df_provisions.rename(columns={"organisation": "organisation_ref"}, inplace=True)
    df_provisions = df_provisions.merge(df_org, on="organisation_ref", how="left")
    df_provisions.drop(columns="organisation_ref", inplace=True)

    # Merge Endpoint with Source and Organisation
    df_ep_org = df_endpoint.merge(df_source, on="endpoint", how="left")
    df_ep_org = df_ep_org.merge(df_org, on="organisation_ref", how="left")
    df_ep_org = df_ep_org[["endpoint", "source", "collection", "organisation"]]

    # Merge Endpoint with Resource and Dataset
    df_ep_ds = df_endpoint.merge(df_resource_endpoint, on="endpoint", how="left")
    df_ep_ds = df_ep_ds.merge(df_resource_dataset, on="resource", how="left")
    df_ep_ds = df_ep_ds[["endpoint", "dataset"]]

    # Final merge of endpoint metadata
    df_final = df_endpoint.merge(df_ep_org, on="endpoint", how="left") 
    df_final = df_final.merge(df_ep_ds, on="endpoint", how="left")
    df_final = df_final[["endpoint", "source", "collection", "endpoint_url", "organisation", "dataset", "end_date"]]

    # Merge with provisioned dataset–organisation combinations
    df_full = df_final.merge(df_provisions, on=["dataset", "organisation"], how="left", indicator=True)

    # Keep only rows not in provision
    df_missing = df_full[df_full["_merge"] == "left_only"].drop(columns=["_merge", "end_date"])

    # Separate PDF rows
    pdf_mask = df_missing["endpoint_url"].fillna("").str.lower().str.endswith(".pdf")
    df_pdfs = df_missing[pdf_mask]
    df_non_pdfs = df_missing[~pdf_mask]

    # Save PDFs separately
    pdf_path = os.path.join(output_dir, "flag_endpoints_pdf_only.csv")
    df_pdfs.to_csv(pdf_path, index=False)

    # Save main CSV (either with or without PDFs)
    if include_pdf:
        final_output = df_missing
    else:
        final_output = df_non_pdfs

    csv_path = os.path.join(output_dir, "flag_endpoints_no_provision.csv")
    final_output.to_csv(csv_path, index=False)

def parse_args():
    """
    Parses command-line arguments for specifying the output directory
    and whether to include .pdf endpoint URLs.
    """
    parser = argparse.ArgumentParser(description="Check endpoints missing expected provisions.")
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory to save exported CSVs"
    )
    parser.add_argument(
        "--include-pdf",
        action="store_true",
        help="Include rows where endpoint_url ends in .pdf in main output"
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    endpoint_provisions_check(args.output_dir, include_pdf=True)