# Job Seek

**Job-seek** is a Streamlit app that scrapes company career pages and ATS job boards, normalizes the results, and shows them in a lightweight dashboard with a **“newest jobs”** overview. It uses a general extractor pipeline (anchors, JSON-LD, repeated blocks, pagination/normalization) and falls back to **headless JS rendering** when a page is a JS shell. A **failure-resistant data model** tracks attempts and health to avoid flakiness across runs.

<img width="1039" height="735" alt="image" src="https://github.com/user-attachments/assets/3cc3318b-dd14-495d-8b1a-6ec429b649d5" />  
<img width="1056" height="580" alt="image" src="https://github.com/user-attachments/assets/7c2f5d95-c449-4c0d-ba81-ea31ec2d5fac" />  



### Core features

* **General scraping pipeline.** Generic extractors (anchor, JSON-LD, list items, repeated block patterns) + pagination & URL normalization aim to work across most job boards.
* **JS-rendered pages support.** If a page looks like a JS shell, the scraper fetches the **rendered HTML** as a fallback.
* **Failure resistance.** The data model keeps per-board scrape attempts/health and merges results conservatively to reduce bad updates when the scrapes fail.
* **Dashboard UI.** A Streamlit interface renders job cards and a simple page to browse results — including a **newest-first** overview.

<img width="765" height="562" alt="image" src="https://github.com/user-attachments/assets/e28b8497-f0c9-44f8-8a4a-3240c33710c9" />  

### Custom scrapers (when general rules aren’t enough)

Most sites work with the general pipeline, but some require dedicated adapters. Currently implemented:

* **Lever**
* **Meta Careers**
* **Microsoft Careers**
* **Proton** (Greenhouse, with CH-focused location terms)
* **Workday**
* **Join**
* **Greenhouse**
* **Ashby**

These are wired via the `custom` adapters registry.

### Running the app

```bash
# 1) Install deps (recommended: use a venv)
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2) Start the UI
streamlit run app.py
```

The directory `job_seek_seed` includes pre-made pages configurations for scraping.

### Using the job\_seek\_seed presets (optional, recommended)

The directory `job_seek_seed` contains **pre-made page configurations**. Normally, you would populate the list of pages to scrape yourself (via the UI or by adding configs under `data/pages`). To get started fast, use a curated preset:

* `job_seek_seed/mle_swe_at_switzerland` focuses on the **Swiss MLE/SWE job market**.

Copy the preset pages into your data folder:

**macOS/Linux:**

```bash
mkdir -p data/pages
cp -r job_seek_seed/mle_swe_at_switzerland/pages/* data/pages/
```

**Windows (PowerShell):**

```powershell
New-Item -ItemType Directory -Force -Path data\pages | Out-Null
Copy-Item -Recurse -Force job_seek_seed\mle_swe_at_switzerland\pages\* data\pages\
```

Then run the app. The dashboard will load these pages and show a **newest jobs** overview. You can extend or replace the list by adding your own page configs under `data/pages` or through the UI.

### Contributing

* **All contributions are welcome.** Bug fixes, docs, small tweaks—everything helps.
* **Scrapers need constant care.** New **custom scrapers** and **updates to existing ones** (when page structures change) are especially valuable.
* **Seed lists matter.** Please add **new seeds** (e.g., curated page lists) and **refresh older seeds** to keep coverage complete and current.

Quick guide: fork → create a feature branch → commit with a clear message → open a PR describing what changed and how to test it.

