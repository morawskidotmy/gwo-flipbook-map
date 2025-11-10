import json
import os
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import requests

NUM_WORKERS   = int(os.getenv("NUM_WORKERS", "8"))
JSON_FILE     = Path(os.getenv("JSON_FILE", "results.json"))
PROGRESS_FILE = Path(os.getenv("PROGRESS_FILE", "progress_download.json"))
BASE_OUT      = Path(os.getenv("BASE_OUT", "downloads"))
START_PAGE    = int(os.getenv("START_PAGE", "1"))
MAX_RETRIES   = int(os.getenv("MAX_RETRIES", "3"))
SLEEP_BETWEEN = float(os.getenv("SLEEP_BETWEEN", "0.2"))

if not JSON_FILE.exists():
    raise FileNotFoundError(f"Missing list file: {JSON_FILE}")

if not PROGRESS_FILE.exists():
    PROGRESS_FILE.write_text(json.dumps({}), encoding="utf-8")

json_lock = threading.Lock()
prog_lock = threading.Lock()

def _write_atomic(path: Path, data: dict) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def load_progress() -> dict:
    with PROGRESS_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_progress(state: dict) -> None:
    with prog_lock:
        _write_atomic(PROGRESS_FILE, state)

def image_url(display_url: str, page: int) -> str:
    book_id = display_url.rstrip("/").split("/")[-1]
    return f"https://flipbook.apps.gwo.pl/book/getImage/bookId:{book_id}/pageNo:{page}"

def fetch_image(url: str, dest: Path) -> bool:
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        dest.write_bytes(r.content)
        return True
    except requests.RequestException:
        return False

def download_iter(iter_id: int, display_url: str, title: str, start_page: int, progress: dict) -> None:
    out_dir = BASE_OUT / f"{iter_id}_{title.replace(' ', '_')}"
    out_dir.mkdir(parents=True, exist_ok=True)

    page = start_page
    while True:
        img_url = image_url(display_url, page)
        out_file = out_dir / f"page_{page:04d}.jpg"

        ok = False
        for attempt in range(1, MAX_RETRIES + 1):
            if fetch_image(img_url, out_file):
                ok = True
                break
            print(f"[Iter {iter_id}] page {page} – retry {attempt}/{MAX_RETRIES}")
            time.sleep(SLEEP_BETWEEN)

        if not ok:
            print(f"[Iter {iter_id}] finished at page {page - 1}")
            break

        with prog_lock:
            progress[str(iter_id)] = page
        save_progress(progress)

        print(f"[Iter {iter_id}] downloaded page {page}")
        page += 1
        time.sleep(SLEEP_BETWEEN)

def main():
    with JSON_FILE.open("r", encoding="utf-8") as f:
        items = json.load(f)

    progress = load_progress()

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = []
        for entry in items:
            iter_id = entry["iter"]
            display = entry["url"]
            title = entry.get("title", f"iter_{iter_id}")

            last_page = progress.get(str(iter_id), 0)
            start = last_page + 1 if last_page else START_PAGE

            futures.append(
                executor.submit(
                    download_iter,
                    iter_id,
                    display,
                    title,
                    start,
                    progress,
                )
            )

        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:
                print(f"⚠️  Worker raised: {exc}")

    print("\n✅ All iter downloads finished (or exhausted).")

if __name__ == "__main__":
    main()
