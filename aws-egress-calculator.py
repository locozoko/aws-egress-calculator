import boto3
import pandas as pd
import os
import sys
import time
import atexit
from datetime import datetime, timezone
import dateutil.relativedelta

output_file = None

def print_both(text="", end="\n", flush=False):
    print(text, end=end, flush=flush)
    if output_file:
        output_file.write(text + end)

success_flag = False

NAT_CACHE_FILE = 'nat_org_usage_cache.csv'
DTO_CACHE_FILE = 'dto_usage_cache.csv'

nat_cache_df = pd.DataFrame()
dto_cache_df = pd.DataFrame()
resume_mode = False

# Constants
NAMESPACE_NAT = 'AWS/NATGateway'
NAMESPACE_BILLING = 'AWS/Billing'
METRIC_NAT_UPLOAD = 'BytesInFromSource'
METRIC_NAT_DOWNLOAD = 'BytesInFromDestination'
METRIC_DTO = 'AWS:DataTransfer-Out-Bytes'
PERIOD = 86400
CACHE_FILE = 'nat_org_usage_cache.csv'

# --- Cache Utility Functions --- #

def load_cache(cache_file):
    if os.path.exists(cache_file):
        try:
            return pd.read_csv(cache_file)
        except:
            return pd.DataFrame()
    else:
        return pd.DataFrame()

def save_to_cache(df, cache_file):
    df.to_csv(cache_file, index=False)

# ----------- Utility Functions ------------

def get_monthly_ranges(months_back=12):
    now = datetime.now(timezone.utc)
    end = now.replace(day=1)
    ranges = [
        (
            end - dateutil.relativedelta.relativedelta(months=i + 1),
            end - dateutil.relativedelta.relativedelta(months=i)
        )
        for i in range(months_back)
    ][::-1]
    ranges.append((end, now))
    return ranges

def prompt_for_regions():
    ec2 = boto3.client('ec2')
    all_regions = [r['RegionName'] for r in ec2.describe_regions()['Regions']]
    print_both("\nSelect a region to query:")
    print_both("0) All regions")
    for i, r in enumerate(all_regions, start=1):
        print_both(f"{i}) {r}")
    choice = input("Enter choice: ").strip()

    if choice == "0":
        return all_regions
    try:
        idx = int(choice)
        return [all_regions[idx - 1]]
    except:
        print_both("Invalid selection.")
        sys.exit(1)

def get_all_accounts():
    try:
        org = boto3.client('organizations')
        accounts = []
        paginator = org.get_paginator('list_accounts')
        for page in paginator.paginate():
            for acct in page['Accounts']:
                if acct['Status'] == 'ACTIVE':
                    accounts.append(acct['Id'])
        return accounts
    except Exception as e:
        print_both(f"[!] Failed to list AWS accounts via Organizations: {e}")
        ids = input("Enter comma-separated list of AWS Account IDs to check: ").strip()
        return [a.strip() for a in ids.split(',') if a.strip()]

def assume_role(account_id, role_name):
    sts = boto3.client('sts')
    try:
        response = sts.assume_role(
            RoleArn=f"arn:aws:iam::{account_id}:role/{role_name}",
            RoleSessionName="OrgNATMetricsSession"
        )
        creds = response['Credentials']
        return boto3.Session(
            aws_access_key_id=creds['AccessKeyId'],
            aws_secret_access_key=creds['SecretAccessKey'],
            aws_session_token=creds['SessionToken']
        )
    except Exception as e:
        print_both(f"  [!] Failed to assume role in {account_id}: {e}")
        return None

def discover_nat_ids(cw):
    seen = set()
    paginator = cw.get_paginator('list_metrics')
    for page in paginator.paginate(Namespace=NAMESPACE_NAT, MetricName=METRIC_NAT_UPLOAD):
        for metric in page['Metrics']:
            for d in metric['Dimensions']:
                if d['Name'] == 'NatGatewayId':
                    seen.add(d['Value'])
    return list(seen)

def get_metric_sum(cw, namespace, metric_name, dimensions, start, end):
    try:
        resp = cw.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=dimensions,
            StartTime=start,
            EndTime=end,
            Period=PERIOD,
            Statistics=['Sum']
        )
        return sum(dp['Sum'] for dp in resp.get('Datapoints', []))
    except:
        return 0

# ------------ Main Query Functions ------------
def is_management_account():
    try:
        org = boto3.client('organizations')
        identity = boto3.client('sts').get_caller_identity()
        response = org.describe_organization()
        return identity['Account'] == response['Organization']['MasterAccountId']
    except Exception:
        return False
    
def get_dto_month_ranges():
    now = datetime.now(timezone.utc)
    end = now.replace(day=1)
    ranges = [
        (
            (end - dateutil.relativedelta.relativedelta(months=i + 1)).strftime('%Y-%m-%d'),
            (end - dateutil.relativedelta.relativedelta(months=i)).strftime('%Y-%m-%d')
        )
        for i in range(12)
    ][::-1]
    ranges.append((
        end.strftime('%Y-%m-%d'),
        now.strftime('%Y-%m-%d')
    ))
    return ranges

def run_dto_query(use_org, regions):
    global dto_cache_df  # <-- move this to top
    ce = boto3.client('ce')
    month_ranges = get_dto_month_ranges()
    dto_monthly = {}

    try:
        for start, end in month_ranges:
            month_label = start[:7]

            # --- RESUME CHECK ---
            if resume_mode:
                cached = dto_cache_df[dto_cache_df['Month'] == month_label]
                if not cached.empty:
                    dto_monthly[month_label] = cached.iloc[0]['GB']
                    continue
            # --------------------

            response = ce.get_cost_and_usage(
                TimePeriod={"Start": start, "End": end},
                Granularity="MONTHLY",
                Metrics=["UsageQuantity"],
                Filter={
                    "Dimensions": {
                        "Key": "USAGE_TYPE",
                        "Values": ["DataTransfer-Out-Bytes"]
                    }
                }
            )
            gb = float(response['ResultsByTime'][0]['Total']['UsageQuantity'].get("Amount", 0))
            dto_monthly[month_label] = gb

            # --- CACHE SAVE ---
            new_row = pd.DataFrame([{'Month': month_label, 'GB': gb}])
            dto_cache_df = pd.concat([dto_cache_df, new_row], ignore_index=True)
            save_to_cache(dto_cache_df, DTO_CACHE_FILE)
            # -----------------

    except Exception as e:
        print_both(f"[!] Warning: Failed to retrieve DTO metrics: {e}")
        return None, {"Total DTO GB": 0, "Average DTO GB per month": 0}

    # Summarize
    total_dto_gb = sum(dto_monthly.values())
    nonzero_months = sum(1 for v in dto_monthly.values() if v > 0)
    avg_dto_gb = total_dto_gb / nonzero_months if nonzero_months > 0 else 0

    return dto_monthly, {
        "Total DTO GB": round(total_dto_gb, 2),
        "Average DTO GB per month": round(avg_dto_gb, 2)
    }

def run_single_account_query(regions, include_dto):
    global nat_cache_df  # <-- ADD THIS AT TOP
    session = boto3.Session()
    account_id = boto3.client('sts').get_caller_identity()['Account']
    cw_clients = {region: session.client('cloudwatch', region_name=region) for region in regions}
    months = get_monthly_ranges()
    nat_results = []
    dto_results = []

    start_time = time.time()
    total_steps = len(months)

    for i, (start, end) in enumerate(months):
        month_label = start.strftime('%Y-%m')
        step_label = f"Processing month: {month_label} ({i+1}/{total_steps})"

        for region in regions:
            cw = cw_clients[region]
            nat_ids = discover_nat_ids(cw)
            if not nat_ids:
                continue

            upload = download = 0
            for nat_id in nat_ids:
                upload += get_metric_sum(cw, NAMESPACE_NAT, METRIC_NAT_UPLOAD, [{'Name': 'NatGatewayId', 'Value': nat_id}], start, end)
                download += get_metric_sum(cw, NAMESPACE_NAT, METRIC_NAT_DOWNLOAD, [{'Name': 'NatGatewayId', 'Value': nat_id}], start, end)

            upload_gb = round(upload / (1024**3), 2)
            download_gb = round(download / (1024**3), 2)
            total_gb = round(upload_gb + download_gb, 2)

            # --- RESUME CHECK ---
            if resume_mode:
                existing = nat_cache_df[
                    (nat_cache_df['Account'] == account_id) &
                    (nat_cache_df['Region'] == region) &
                    (nat_cache_df['Month'] == month_label)
                ]
                if not existing.empty:
                    continue
            # --------------------

            nat_results.append({
                'Account': account_id,
                'Region': region,
                'Month': month_label,
                'Upload GB': upload_gb,
                'Download GB': download_gb,
                'Total GB': total_gb,
            })

            # --- CACHE SAVE ---
            new_row = pd.DataFrame([{
                'Account': account_id,
                'Region': region,
                'Month': month_label,
                'Upload GB': upload_gb,
                'Download GB': download_gb,
                'Total GB': total_gb,
            }])
            nat_cache_df = pd.concat([nat_cache_df, new_row], ignore_index=True)
            save_to_cache(nat_cache_df, NAT_CACHE_FILE)
            # ------------------

        # Progress bar
        elapsed = time.time() - start_time
        remaining = max(0, int((elapsed / (i + 1)) * (total_steps - (i + 1))))
        bar = "[" + "#" * int((i+1)/total_steps*30) + "-" * (30 - int((i+1)/total_steps*30)) + "]"
        print_both(f"\r{step_label} {bar} Est. time remaining: {remaining} sec", end="", flush=True)

    print_both()
    nat_df = pd.DataFrame(nat_results)
    dto_df = pd.DataFrame(dto_results)
    return nat_df, dto_df

def run_org_query(accounts, role_name, regions, include_dto):
    global nat_cache_df  # <-- ADD THIS AT TOP
    months = get_monthly_ranges()
    nat_results = []

    start_time = time.time()
    total_steps = len(months)

    for i, (start, end) in enumerate(months):
        month_label = start.strftime('%Y-%m')
        label = f"Processing month: {month_label} ({i+1}/{total_steps})"

        for account_id in accounts:
            session = assume_role(account_id, role_name)
            if not session:
                continue

            for region in regions:
                cw = session.client('cloudwatch', region_name=region)
                nat_ids = discover_nat_ids(cw)
                if not nat_ids:
                    continue

                upload = download = 0
                for nat_id in nat_ids:
                    upload += get_metric_sum(cw, NAMESPACE_NAT, METRIC_NAT_UPLOAD, [{'Name': 'NatGatewayId', 'Value': nat_id}], start, end)
                    download += get_metric_sum(cw, NAMESPACE_NAT, METRIC_NAT_DOWNLOAD, [{'Name': 'NatGatewayId', 'Value': nat_id}], start, end)

                upload_gb = round(upload / (1024**3), 2)
                download_gb = round(download / (1024**3), 2)
                total_gb = round(upload_gb + download_gb, 2)

                # --- RESUME CHECK ---
                if resume_mode:
                    existing = nat_cache_df[
                        (nat_cache_df['Account'] == account_id) &
                        (nat_cache_df['Region'] == region) &
                        (nat_cache_df['Month'] == month_label)
                    ]
                    if not existing.empty:
                        continue
                # --------------------

                nat_results.append({
                    'Account': account_id,
                    'Region': region,
                    'Month': month_label,
                    'Upload GB': upload_gb,
                    'Download GB': download_gb,
                    'Total GB': total_gb,
                })

                # --- CACHE SAVE ---
                new_row = pd.DataFrame([{
                    'Account': account_id,
                    'Region': region,
                    'Month': month_label,
                    'Upload GB': upload_gb,
                    'Download GB': download_gb,
                    'Total GB': total_gb,
                }])
                nat_cache_df = pd.concat([nat_cache_df, new_row], ignore_index=True)
                save_to_cache(nat_cache_df, NAT_CACHE_FILE)
                # ------------------

        elapsed = time.time() - start_time
        remaining = max(0, int((elapsed / (i + 1)) * (total_steps - (i + 1))))
        bar = "[" + "#" * int((i+1)/total_steps*30) + "-" * (30 - int((i+1)/total_steps*30)) + "]"
        print_both(f"\r{label} {bar} Est. time remaining: {remaining} sec", end="", flush=True)

    print_both()
    nat_df = pd.DataFrame(nat_results)
    return nat_df, None

# ------------ Output Section ------------

def print_summary(accounts, regions, use_org, include_dto, nat_totals, dto_totals):
    print_both("\n=== Analysis Summary ===")
    print_both(f"- AWS Organizations mode: {'Yes' if use_org else 'No'}")
    print_both(f"- NAT Gateway Analysis: Yes")
    print_both(f"- DTO Analysis: {'Yes' if include_dto else 'No'}")
    print_both(f"- AWS Accounts analyzed: {', '.join(accounts)}")
    print_both(f"- Regions analyzed: {', '.join(regions)}")

    # If DTO enabled, show difference vs egress
    if include_dto:
        nat_egress = nat_totals["Total NAT Upload GB"]
        dto_total = dto_totals["Total DTO GB"]
        diff = dto_total - nat_egress
        print_both("\n=== DTO vs NAT Upload Comparison ===")
        print_both("\n- Total DTO (Data Transfer Out) traffic over last 12 months: {:.2f} GB".format(dto_totals["Total DTO GB"]))
        print_both("- Average monthly DTO traffic for quoting: {:.2f} GB".format(dto_totals["Average DTO GB per month"]))
        print_both("- Difference (DTO vs NAT Egress): {:.2f} GB".format(diff))

    print_both("\n=== NAT Gateway Traffic Breakdown ===")

    print_both("- Total NAT Gateway Upload (egress) over last 12 months: {:.2f} GB".format(nat_totals["Total NAT Upload GB"]))
    print_both("- Average monthly NAT Gateway Upload: {:.2f} GB".format(nat_totals["Average NAT Upload GB per month"]))

    print_both("- Total NAT Gateway Download (ingress) over last 12 months: {:.2f} GB".format(nat_totals["Total NAT Download GB"]))
    print_both("- Average monthly NAT Gateway Download: {:.2f} GB".format(nat_totals["Average NAT Download GB per month"]))

    # Existing Total NAT traffic
    print_both("\n- Total NAT Gateway traffic over last 12 months: {:.2f} GB".format(nat_totals["Total NAT GB"]))
    print_both("- Average monthly NAT Gateway traffic for quoting: {:.2f} GB".format(nat_totals["Average NAT GB per month"]))

# ------------ Runner ------------
def run_nat_query(use_org, accounts, role_name, regions, include_dto):
    if use_org:
        df, _ = run_org_query(accounts, role_name, regions, include_dto)
    else:
        df, _ = run_single_account_query(regions, include_dto)

    if df.empty:
        return df, {"Total NAT GB": 0, "Average NAT GB per month": 0}

    all_months_sorted = sorted(df["Month"].unique())
    last_12_months = all_months_sorted[-12:]
    df_nonzero = df[df["Month"].isin(last_12_months) & (df["Total GB"] > 0)]

    total_nat_gb = df_nonzero["Total GB"].sum()
    total_nat_upload = df_nonzero["Upload GB"].sum()
    total_nat_download = df_nonzero["Download GB"].sum()
    months_nonzero = df_nonzero["Month"].nunique()

    avg_nat_gb = total_nat_gb / months_nonzero if months_nonzero > 0 else 0
    avg_nat_upload = total_nat_upload / months_nonzero if months_nonzero > 0 else 0
    avg_nat_download = total_nat_download / months_nonzero if months_nonzero > 0 else 0

    avg_nat_gb = total_nat_gb / months_nonzero if months_nonzero > 0 else 0
    avg_nat_upload = total_nat_upload / months_nonzero if months_nonzero > 0 else 0
    avg_nat_download = total_nat_download / months_nonzero if months_nonzero > 0 else 0

    return df, {
        "Total NAT GB": total_nat_gb,
        "Total NAT Upload GB": total_nat_upload,
        "Total NAT Download GB": total_nat_download,
        "Average NAT Upload GB per month": avg_nat_upload,
        "Average NAT Download GB per month": avg_nat_download,
        "Average NAT GB per month": avg_nat_gb
    }

def run_aws_analysis():
    global success_flag
    global nat_cache_df, dto_cache_df, resume_mode

    if os.path.exists(NAT_CACHE_FILE) or os.path.exists(DTO_CACHE_FILE):
        choice = input("\n[!] Existing cache detected. Resume from previous run? (y/n): ").strip().lower()
        if choice == 'y':
            resume_mode = True
            if os.path.exists(NAT_CACHE_FILE):
                nat_cache_df = load_cache(NAT_CACHE_FILE)
                print_both(f"[✓] Loaded {len(nat_cache_df)} cached NAT entries from {NAT_CACHE_FILE}")
            if os.path.exists(DTO_CACHE_FILE):
                dto_cache_df = load_cache(DTO_CACHE_FILE)
                print_both(f"[✓] Loaded {len(dto_cache_df)} cached DTO entries from {DTO_CACHE_FILE}")
            print_both("\n[✓] Resuming from previous run.\n")
        else:
            try:
                if os.path.exists(NAT_CACHE_FILE):
                    os.remove(NAT_CACHE_FILE)
                if os.path.exists(DTO_CACHE_FILE):
                    os.remove(DTO_CACHE_FILE)
                print_both("\n[✓] Old cache files deleted. Starting fresh run.\n")
            except Exception as e:
                print_both(f"[!] Failed to delete cache files: {e}")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    outfile_name = f"aws-output-{timestamp}.txt"
    global output_file
    output_file = open(outfile_name, "w")
    print_both(f"\n[✓] Output will also be saved to: {outfile_name}\n")

    # ---> First ask user
    use_org = input("Run against AWS Organizations? (y/n): ").strip().lower() == "y"
    include_dto = input("Include DTO (Data Transfer Out) metrics? (y/n): ").strip().lower() == "y"

    # ---> Now it's safe to check
    if use_org and include_dto:
        print_both("\n[!] Note: DTO metrics are pulled from the authenticated account. If you want full Org-wide DTO analysis, run the script from the management (payer) account with billing permissions.\n")

    regions = prompt_for_regions()

    if include_dto and use_org:
        if not is_management_account():
            print_both("\n[!] Note: DTO (Data Transfer Out) metrics are only collected from the current account.")
            print_both("    To get full DTO across all Org accounts, run this script from the AWS Organizations management account.\n")

    role_name = None
    accounts = []

    if use_org:
        role_name = input("Enter IAM role name to assume in each account: ").strip()
        try:
            accounts = get_all_accounts()
        except:
            ids = input("Enter comma-separated list of AWS Account IDs to check: ").strip()
            accounts = [a.strip() for a in ids.split(',') if a.strip()]
    else:
        session = boto3.Session()
        account_id = boto3.client('sts').get_caller_identity()['Account']
        accounts = [account_id]

    nat_df, nat_totals = run_nat_query(use_org, accounts, role_name, regions, include_dto)

    if include_dto:
        dto_df, dto_totals = run_dto_query(use_org, regions)
    else:
        dto_totals = {
            "Total DTO GB": 0,
            "Average DTO GB per month": 0
        }

    if nat_df.empty:
        print_both("No NAT Gateway usage data found.")
        sys.exit(0)

    success_flag = True
    print_both("\n=== ORG NAT + DTO Usage Summary ===\n")
    print_summary(accounts, regions, use_org, include_dto, nat_totals, dto_totals)
    show_details = input("\nShow detailed tables per AWS account and region? (y/n): ").strip().lower() == "y"
    if show_details:
        print_detailed_tables(nat_df)

def print_summary(accounts, regions, use_org, include_dto, nat_totals, dto_totals):
    print_both("\n=== Analysis Summary ===")
    print_both(f"- AWS Organizations mode: {'Yes' if use_org else 'No'}")
    print_both(f"- NAT Gateway Analysis: Yes")
    print_both(f"- DTO Analysis: {'Yes' if include_dto else 'No'}")
    print_both(f"- AWS Accounts analyzed: {', '.join(accounts)}")
    print_both(f"- Regions analyzed: {', '.join(regions)}")

    if include_dto:
        print_both("\n[!] Reminder: DTO data shown below is based on billing visibility from the current account session. For Org-wide DTO totals, run from the management account.\n")

        nat_egress = nat_totals["Total NAT Upload GB"]
        dto_total = dto_totals["Total DTO GB"]
        diff = dto_total - nat_egress
        print_both("\n=== DTO vs NAT Upload Comparison ===")
        print_both("\n- Total DTO (Data Transfer Out) traffic over last 12 months: {:.2f} GB".format(dto_totals["Total DTO GB"]))
        print_both("- Average monthly DTO traffic for quoting: {:.2f} GB".format(dto_totals["Average DTO GB per month"]))
        print_both("- Difference (DTO vs NAT Egress): {:.2f} GB".format(diff))

    print_both("\n=== NAT Gateway Traffic Breakdown ===")

    print_both("- Total NAT Gateway Upload (egress) over last 12 months: {:.2f} GB".format(nat_totals["Total NAT Upload GB"]))
    print_both("- Average monthly NAT Gateway Upload: {:.2f} GB".format(nat_totals["Average NAT Upload GB per month"]))

    print_both("- Total NAT Gateway Download (ingress) over last 12 months: {:.2f} GB".format(nat_totals["Total NAT Download GB"]))
    print_both("- Average monthly NAT Gateway Download: {:.2f} GB".format(nat_totals["Average NAT Download GB per month"]))

    print_both("\n- Total NAT Gateway traffic over last 12 months: {:.2f} GB".format(nat_totals["Total NAT GB"]))
    print_both("- Average monthly NAT Gateway traffic for quoting: {:.2f} GB".format(nat_totals["Average NAT GB per month"]))

def print_detailed_tables(df):
    # Get last 12 full months only
    all_months_sorted = sorted(df["Month"].unique())
    last_12_months = all_months_sorted[-12:]
    df = df[df["Month"].isin(last_12_months)]

    accounts = df["Account"].unique()

    for account in accounts:
        print_both(f"\n=== Detailed NAT Gateway Traffic for Account: {account} ===")
        subset = df[df["Account"] == account]

        pivot = subset.pivot_table(
            index="Region",
            columns="Month",
            values="Total GB",
            aggfunc="sum",
            fill_value=0
        )

        # Add a total row
        pivot.loc["Total"] = pivot.sum()

        # Calculate grand total across all regions and months
        grand_total = pivot.loc["Total"].sum()

        # Print the table and grand total
        print_both(pivot.round(2).to_string())
        print_both(f"\n[✓] Grand Total NAT traffic for {account}: {grand_total:.2f} GB")

@atexit.register
def cleanup_cache():
    global success_flag, output_file
    try:
        if success_flag:
            if os.path.exists(NAT_CACHE_FILE):
                os.remove(NAT_CACHE_FILE)
            if os.path.exists(DTO_CACHE_FILE):
                os.remove(DTO_CACHE_FILE)
            print_both(f"\n[✓] Deleted cache files after successful run.")
    except Exception as e:
        print_both(f"\n[!] Could not delete cache files: {e}")

    # Finally close output file (after all prints are done)
    if output_file:
        try:
            output_file.close()
        except Exception as e:
            print(f"[!] Could not close output file cleanly: {e}")

# Entry
if __name__ == "__main__":
    run_aws_analysis()