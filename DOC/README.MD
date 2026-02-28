# Bleems.com Scraper

A production-ready web scraper for [Bleems.com](https://www.bleems.com) that extracts shop information, products, reviews, and images, then uploads everything to Amazon S3 with Hive-style partitioning.

## 🚀 Features

- **Comprehensive Data Extraction**
  - Shop details (name, type, rating, logo)
  - Product information (name, price, category, images)
  - Customer reviews (text, rating, reviewer name, date)
  - Images (shop logos and product images)

- **Intelligent Parsing**
  - Handles Arabic and English text encoding correctly
  - Multiple fallback strategies for HTML parsing
  - Extracts data from JavaScript objects and AJAX endpoints
  - CSRF token handling for ASP.NET Core endpoints

- **AWS S3 Integration**
  - Hive-style partitioning by date and category
  - UTF-8 with BOM for Excel compatibility
  - Automatic image upload with proper content types
  - Organized folder structure

- **Parallel Processing**
  - Dynamic category discovery
  - GitHub Actions matrix strategy for parallel execution
  - ~6 hours sequential → ~40 minutes parallel
  - Fault-tolerant (one category failure doesn't stop others)

## 📁 Project Structure

```
Project6-BL/
├── scraper.py              # Main scraper script
├── requirements.txt        # Python dependencies
├── README.md              # This file
└── .github/
    └── workflows/
        └── scrape.yml     # GitHub Actions workflow
```

## 🗂️ S3 Output Structure

```
s3://your-bucket/
└── bleems-data/
    └── year=2026/
        └── month=02/
            └── day=28/
                ├── Flowers/
                │   ├── shops.csv          # Shop details
                │   ├── items.csv          # Product details
                │   ├── reviews.csv        # Customer reviews
                │   └── images/
                │       ├── Shop_Name_1/
                │       │   ├── logo/
                │       │   │   └── logo.jpg
                │       │   └── products/
                │       │       ├── 311913.jpg
                │       │       └── 311914.jpg
                │       └── Shop_Name_2/
                │           └── ...
                ├── Confections/
                │   └── ...
                └── Gifts/
                    └── ...
```

## 📊 CSV Schema

### shops.csv
| Column | Description |
|--------|-------------|
| name | Shop name |
| type | Category (Flowers, Confections, Gifts) |
| rating | Average rating (1-5 stars) |
| ratings_count | Number of ratings |
| slug | URL slug |
| url | Full shop URL |
| logo_url | Original logo URL |
| s3_image_path | S3 path to uploaded logo |
| scraped_date | Date of scraping |

### items.csv
| Column | Description |
|--------|-------------|
| shop_name | Parent shop name |
| shop_type | Shop category |
| product_id | Unique product ID |
| product_name | Product name |
| category | Product category |
| brand | Brand name |
| price | Price value |
| currency | Currency (KWD) |
| occasion | Associated occasion |
| product_type | Product type |
| sub_category | Sub-category |
| flavors | Available flavors (comma-separated) |
| colors | Available colors (comma-separated) |
| product_url | Product page URL |
| image_url | Original product image URL |
| s3_image_path | S3 path to uploaded image |

### reviews.csv
| Column | Description |
|--------|-------------|
| shop_name | Parent shop name |
| shop_type | Shop category |
| reviewer_name | Reviewer name |
| review_date | Review date |
| review_text | Review content (Arabic/English) |
| star_rating | Rating (1-5 stars) |
| scraped_date | Date of scraping |

## 🛠️ Setup

### Prerequisites

- Python 3.12+
- AWS account with S3 access
- GitHub account (for automated runs)

### Local Installation

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd Project6-BL
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure AWS credentials**
   ```bash
   # Option 1: Environment variables
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export S3_BUCKET_NAME=your_bucket_name
   
   # Option 2: AWS CLI configuration
   aws configure
   ```

## 🎯 Usage

### Local Execution

**Scrape all categories:**
```bash
python scraper.py
```

**Scrape specific category:**
```bash
python scraper.py --category "Flowers"
python scraper.py --category "Confections"
python scraper.py --category "Gifts"
```

**List available categories:**
```bash
python scraper.py --list-categories
# Output: ["Flowers", "Confections", "Gifts"]
```

### GitHub Actions (Automated)

#### Setup

1. **Add repository secrets** (Settings → Secrets → Actions)
   - `AWS_ACCESS_KEY_ID` - Your AWS access key
   - `AWS_SECRET_ACCESS_KEY` - Your AWS secret key
   - `S3_BUCKET_NAME` - Your S3 bucket name

2. **Workflow triggers automatically**
   - **Daily**: Every day at 02:00 UTC
   - **Manual**: Actions tab → "Bleems Scraper → S3" → "Run workflow"

#### How It Works

```
┌─────────────────────────────────────────────┐
│  Trigger (Schedule/Manual)                  │
└────────────────┬────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────┐
│  Job 1: Discover Categories (~30 seconds)  │
│  - Fetches shop list                        │
│  - Extracts unique categories               │
│  - Outputs: ["Flowers", "Confections", ...]│
└────────────────┬────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────┐
│  Job 2: Parallel Scraping (3 jobs)         │
│  ┌─────────────────────────────────────┐   │
│  │  Scrape Flowers     (30 min)       │   │
│  ├─────────────────────────────────────┤   │
│  │  Scrape Confections (25 min)       │   │
│  ├─────────────────────────────────────┤   │
│  │  Scrape Gifts       (40 min)       │   │
│  └─────────────────────────────────────┘   │
│                                             │
│  Total time: ~40 min (slowest job)         │
└─────────────────────────────────────────────┘
```

#### Monitor Progress

- Go to **Actions** tab in GitHub
- Click on the latest workflow run
- View logs for each category separately
- Check S3 verification output at the end

## 🔧 Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AWS_ACCESS_KEY_ID` | Yes | - | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | - | AWS secret key |
| `AWS_DEFAULT_REGION` | No | `us-east-1` | AWS region |
| `S3_BUCKET_NAME` | Yes | - | S3 bucket name |

### Scraper Settings

Edit `scraper.py` to customize:

```python
# Request delay between pages (seconds)
REQUEST_DELAY = 1.5

# Base URL and country
BASE_URL = "https://www.bleems.com"
COUNTRY = "kw"

# S3 folder prefix
S3_FOLDER = "bleems-data"
```

## 🐛 Troubleshooting

### Arabic Text Shows as Garbled Characters

**Issue**: Reviews appear as `Ø­Ù„Ùˆ` instead of `حلو`

**Solution**: The scraper uses UTF-8 with BOM. Make sure you're opening CSV files with:
- Excel: Should auto-detect
- Google Sheets: File → Import → Upload → Detect encoding automatically
- Pandas: `pd.read_csv('file.csv', encoding='utf-8-sig')`

### Reviews Not Loading

**Issue**: `0 reviews for {shop_name}`

**Causes**:
1. **No reviews**: Shop genuinely has no reviews
2. **AJAX endpoint**: Script handles this automatically with CSRF tokens
3. **Rate limiting**: Script includes delays, but if blocked, increase `REQUEST_DELAY`

**Debug**:
```bash
# Enable debug mode locally
export DEBUG_HTML=1
python scraper.py --category "Flowers"
```

### Images Not Uploading

**Issue**: `s3_image_path` column is empty

**Causes**:
1. **Missing image URL**: Product has no image
2. **S3 permissions**: Check IAM policy allows `s3:PutObject`
3. **Network timeout**: Increase timeout in `upload_image_to_s3()`

**Verify**:
```bash
aws s3 ls s3://your-bucket/bleems-data/ --recursive | grep images
```

### GitHub Actions Timeout

**Issue**: Job exceeds 6-hour default timeout

**Solution**: Already handled! Workflow runs categories in parallel:
- Old: 6+ hours sequential
- New: ~40 minutes parallel

### Dynamic Categories Not Working

**Issue**: Workflow still uses hardcoded categories

**Solution**: 
1. Ensure `--list-categories` works locally
2. Check "Discover Categories" job output in GitHub Actions
3. Verify `needs.discover.outputs.categories` in workflow

## 📈 Performance

### Metrics (Approximate)

| Metric | Value |
|--------|-------|
| Total shops | ~266 |
| Products per shop | ~20 |
| Reviews per shop | ~0-200 |
| Total requests | ~6,000+ |
| Sequential time | 6+ hours |
| **Parallel time** | **~40 minutes** |
| Data size | ~50-100 MB/day |

### Optimization Tips

1. **Reduce delay**: Only if not rate-limited
   ```python
   REQUEST_DELAY = 1.0  # Default: 1.5
   ```

2. **Parallel product fetching**: Advanced (requires thread safety)

3. **Resume capability**: Track processed shops to resume on failure

4. **Incremental scraping**: Only scrape changed/new shops

## 🔒 Security

- **Never commit credentials**: Use secrets management
- **IAM best practices**: Use least-privilege S3 policies
- **Rate limiting**: Respect website's rate limits
- **User-Agent**: Script identifies itself properly

### Recommended IAM Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name",
        "arn:aws:s3:::your-bucket-name/*"
      ]
    }
  ]
}
```

## 📝 License

This project is for educational/research purposes. Please respect Bleems.com's terms of service and robots.txt.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📧 Support

For issues or questions:
1. Check the [Troubleshooting](#-troubleshooting) section
2. Review GitHub Actions logs
3. Open an issue with:
   - Error message
   - Relevant logs
   - Steps to reproduce

## 🎓 Technical Details

### Technologies Used

- **Python 3.12**: Core language
- **requests**: HTTP client with retry logic
- **BeautifulSoup4 + lxml**: HTML/XML parsing
- **pandas**: Data manipulation and CSV export
- **boto3**: AWS S3 integration
- **GitHub Actions**: CI/CD and automation

### Key Algorithms

1. **JS-to-JSON Converter**: Handles single-quoted JavaScript objects
2. **Dual-Strategy Parser**: lxml + regex fallback for malformed HTML
3. **CSRF Token Extraction**: ASP.NET Core antiforgery handling
4. **Dynamic Category Discovery**: Auto-adapts to website changes
5. **UTF-8 BOM Encoding**: Excel-compatible CSV export

### Architecture Patterns

- **Retry Logic**: Exponential backoff for failed requests
- **Session Management**: Isolated sessions per shop for reviews
- **Explicit Encoding**: UTF-8 at every layer to prevent corruption
- **Fault Tolerance**: Per-category failure isolation
- **Idempotent Uploads**: Same date/category overwrites previous data

---

**Last Updated**: February 28, 2026
