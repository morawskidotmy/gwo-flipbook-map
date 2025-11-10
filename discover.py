#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, random, threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import requests
from bs4 import BeautifulSoup

NUM_WORKERS   = int(os.getenv("NUM_WORKERS", "8"))
TOTAL_ITER    = int(os.getenv("TOTAL_ITER", "1000000"))
RESULTS_PATH  = Path(os.getenv("RESULTS_PATH", "results.json"))
PROGRESS_PATH = Path(os.getenv("PROGRESS_PATH", "progress.json"))
GWO_PUBLISHER = "Gdańskie Wydawnictwo Oświatowe"
MAX_ID        = int(os.getenv("MAX_ID", "9999999"))
BUFFER_LIMIT  = 50
MAX_CONSECUTIVE_FAILS = 1000

if not RESULTS_PATH.exists():
    RESULTS_PATH.write_text("[]", encoding="utf-8")
if not PROGRESS_PATH.exists():
    PROGRESS_PATH.write_text(json.dumps({"max_iter": -1}), encoding="utf-8")

json_lock   = threading.Lock()
prog_lock   = threading.Lock()
fail_lock   = threading.Lock()
result_buf  = deque()
consecutive_fails = 0

def update_progress(candidate: int) -> bool:
    with prog_lock, PROGRESS_PATH.open("r+", encoding="utf-8") as f:
        prog = json.load(f)
        if candidate > prog.get("max_iter", -1):
            prog["max_iter"] = candidate
            f.seek(0)
            json.dump(prog, f, ensure_ascii=False, indent=2)
            f.truncate()
            return True
    return False

def buffer_result(entry: dict):
    if entry.get("skipped"):
        return
    result_buf.append(entry)
    if len(result_buf) >= BUFFER_LIMIT:
        flush_results()

def flush_results():
    if not result_buf:
        return
    with json_lock, RESULTS_PATH.open("r+", encoding="utf-8") as f:
        data = json.load(f)
        data.extend(result_buf)
        f.seek(0)
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.truncate()
    result_buf.clear()

def record_failure() -> bool:
    global consecutive_fails
    with fail_lock:
        consecutive_fails += 1
        return consecutive_fails >= MAX_CONSECUTIVE_FAILS

def reset_failure_counter():
    global consecutive_fails
    with fail_lock:
        consecutive_fails = 0

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_maxsize=NUM_WORKERS)
session.mount("https://", adapter)

def process_iteration(iter_idx: int):
    url = f"https://flipbook.apps.gwo.pl/display/{random.randint(0, MAX_ID)}"
    try:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
    except requests.HTTPError as http_err:
        if resp.status_code == 404:
            return
        buffer_result({"iter": iter_idx, "url": url, "error": str(http_err)})
        if record_failure():
            raise RuntimeError("1000 consecutive failures reached")
        return
    except requests.RequestException as exc:
        buffer_result({"iter": iter_idx, "url": url, "error": str(exc)})
        if record_failure():
            raise RuntimeError("1000 consecutive failures reached")
        return
    if "text/html" not in resp.headers.get("Content-Type", ""):
        buffer_result({"iter": iter_idx, "url": url, "error": "Non‑HTML response"})
        if record_failure():
            raise RuntimeError("1000 consecutive failures reached")
        return
    soup = BeautifulSoup(resp.text, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else "(no title)"
    if GWO_PUBLISHER in title:
        buffer_result({"iter": iter_idx, "url": url, "title": title, "skipped": True})
        return
    buffer_result({"iter": iter_idx, "url": url, "title": title})
    update_progress(iter_idx)
    reset_failure_counter()

def main():
    start = json.loads(PROGRESS_PATH.read_text(encoding="utf-8")).get("max_iter", -1)
    print(f"Starting from previously recorded max_iter = {start}")
    try:
        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = {executor.submit(process_iteration, i): i for i in range(TOTAL_ITER)}
            for future in as_completed(futures):
                try:
                    future.result()
                except RuntimeError as e:
                    print(f"\n{e}")
                    for f in futures:
                        f.cancel()
                    break
    except KeyboardInterrupt:
        print("\nInterrupted – flushing pending results...")
    finally:
        flush_results()

if __name__ == "__main__":
    main()
