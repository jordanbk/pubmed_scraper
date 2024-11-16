import csv
import requests
import time
from bs4 import BeautifulSoup
import argparse
from tqdm import tqdm

def get_total_records(api_key, keyword, start_date, end_date=None):
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {
        "db": "pubmed",
        "retmode": "xml",
        "term": keyword,
        "mindate": start_date,
        "retmax": 0,
        "api_key": api_key,
        "usehistory": "y"
    }
    if end_date:
        params["maxdate"] = end_date

    response = requests.get(base_url, params=params)
    soup = BeautifulSoup(response.content, "xml")
    count = int(soup.find("Count").text)
    web_env = soup.find("WebEnv").text
    query_key = soup.find("QueryKey").text
    return count, web_env, query_key

def fetch_articles_in_batches(api_key, web_env, query_key, total_records, record_limit=None, batch_size=500, dry_run=False):
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    article_data = []
    record_count = 0
    preview_limit = 10 if dry_run else None  # Limit for dry-run previews

    print(f"Found {total_records} unique articles.")
    if record_limit:
        print(f"Retrieving up to {record_limit} records.")

    total_to_retrieve = min(total_records, record_limit) if record_limit else total_records

    # Set progress bar total for dry-run or full run
    pbar_total = preview_limit if dry_run else total_to_retrieve

    # Initialize progress bar
    with tqdm(total=pbar_total, desc="Fetching articles", unit="record") as pbar:
        for retstart in range(0, total_to_retrieve, batch_size):
            current_batch_size = min(batch_size, total_to_retrieve - record_count)

            params = {
                "db": "pubmed",
                "retmode": "xml",
                "query_key": query_key,
                "WebEnv": web_env,
                "retstart": retstart,
                "retmax": current_batch_size,
                "api_key": api_key,
            }

            response = requests.get(base_url, params=params)
            soup = BeautifulSoup(response.content, "xml")

            articles = soup.find_all("PubmedArticle")
            for article in articles:
                if dry_run and len(article_data) >= preview_limit:
                    pbar.close()  # Close the progress bar
                    print(f"Dry run: Processed {len(article_data)} articles for preview.")
                    return article_data

                if record_limit is not None and record_count >= record_limit:
                    pbar.close()  # Close the progress bar
                    return article_data

                pmid = article.find("PMID").text if article.find("PMID") else "N/A"
                title = article.find("ArticleTitle").text if article.find("ArticleTitle") else "N/A"
                year = article.find("PubDate").find("Year").text if article.find("PubDate") and article.find("PubDate").find("Year") else "N/A"
                authors = article.find_all("Author")

                for author in authors:
                    last_name = author.find("LastName").text if author.find("LastName") else "N/A"
                    fore_name = author.find("ForeName").text if author.find("ForeName") else "N/A"
                    initials = author.find("Initials").text if author.find("Initials") else "N/A"
                    affiliation = author.find("Affiliation").text if author.find("Affiliation") else "N/A"
                    article_data.append([pmid, title, year, last_name, fore_name, initials, affiliation])

                record_count += 1  # Increment for each article processed

            # Update progress bar after processing a batch
            pbar.update(current_batch_size)

            time.sleep(0.1)

    return article_data

def save_to_csv(data, output_file):
    header = ["PMID", "Title", "Year", "Last Name", "First Name", "Initials", "Affiliation"]
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)
        writer.writerows(data)
    print(f"Data saved to {output_file}")
    print(f"Total records saved: {len(data)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A script to fetch PubMed data and save it to a CSV file. "
                    "You can specify keywords, date ranges, and customize the output.",
        epilog="Example usage:\n"
               "  python pubmed_crawler.py --api_key YOUR_API_KEY --keywords cancer health --start_date 2022/01/01 --end_date 2023/01/01 --record_limit 100 --output results.csv",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--api_key", required=False, help="Your PubMed API key. Alternatively, set it as an environment variable.")
    parser.add_argument("--keywords", nargs="+", required=True, help="List of keywords for search. Example: 'cancer' 'health'")
    parser.add_argument("--start_date", required=True, help="Start date for articles in YYYY/MM/DD format.")
    parser.add_argument("--end_date", help="End date for articles in YYYY/MM/DD format. Optional.")
    parser.add_argument("--output", default="pubmed_results.csv", help="Output CSV file. Default: pubmed_results.csv")
    parser.add_argument("--record_limit", type=int, default=None, help="Maximum number of records to retrieve (optional).")
    parser.add_argument("--logic", choices=["AND", "OR", "CUSTOM"], default="AND",
                        help="Logic to apply between keywords. Options: AND, OR, CUSTOM. Default: AND")
    parser.add_argument("--custom_logic", type=str, default=None,
                        help="Custom logic string for combining keywords. Example: '(keyword1 OR keyword2) AND keyword3'")
    parser.add_argument("--dry_run", action="store_true",
                        help="Run the script without saving to a file. Use this to test.")

    args = parser.parse_args()

    # Get API key from command line argument
    api_key = args.api_key
    if not api_key:
        raise ValueError("An API key is required. Pass it with --api_key or set it as an environment variable PUBMED_API_KEY.")

    # Generate keyword combinations based on logic
    if args.logic == "AND":
        keyword_combination = " AND ".join(args.keywords)
    elif args.logic == "OR":
        keyword_combination = " OR ".join(args.keywords)
    elif args.logic == "CUSTOM" and args.custom_logic:
        keyword_combination = args.custom_logic
    else:
        raise ValueError("Invalid logic or custom_logic not provided for CUSTOM logic.")
    
    print(f"Using search term: {keyword_combination}")
    
    # Get total records, WebEnv, and QueryKey
    count, web_env, query_key = get_total_records(api_key, keyword_combination, args.start_date, args.end_date)
    
    # Fetch data
    articles = fetch_articles_in_batches(api_key, web_env, query_key, count, record_limit=args.record_limit, dry_run=args.dry_run)
    
    # Handle dry-run
    if args.dry_run:
        print("Dry run enabled. No data will be saved.")
        print(f"Preview: {articles[:5]}")  # Show the first 5 records as a preview
    else:
        # Save to CSV
        save_to_csv(articles, args.output)
