#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Send the latest EOD ZIP to ChatGPT (OpenAI Assistants API).

Env:
  OPENAI_API_KEY        : Required. Your OpenAI API key.
  OPENAI_ASSISTANT_ID   : Required. Assistant ID configured with file_search tool.
  OPENAI_THREAD_ID      : Optional. Existing thread id to post into (if omitted, a new thread is created).
  ZIP_GLOB              : Optional. Glob pattern to locate ZIP (default: "reports/*.zip").
  MESSAGE_JA            : Optional. Message text in Japanese to send with the ZIP.
"""
import os, glob, json, sys, time
import requests

API = "https://api.openai.com/v1"
HEADERS_JSON = None

def die(msg, code=1):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(code)

def post_json(url, payload):
    r = requests.post(url, headers=HEADERS_JSON, json=payload, timeout=120)
    if r.status_code >= 400:
        die(f"POST {url} failed: {r.status_code} {r.text}")
    return r.json()

def main():
    api_key = os.getenv("OPENAI_API_KEY")
    asst_id = os.getenv("OPENAI_ASSISTANT_ID")
    thread_id = os.getenv("OPENAI_THREAD_ID")
    zip_glob = os.getenv("ZIP_GLOB", "reports/*.zip")
    msg_text = os.getenv("MESSAGE_JA", "本日のEOD ZIPを添付します。ユニバースのファンダメンタル更新と注目銘柄抽出をお願いします。")

    if not api_key:
        die("OPENAI_API_KEY is not set")
    if not asst_id:
        die("OPENAI_ASSISTANT_ID is not set")

    global HEADERS_JSON
    HEADERS_JSON = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    zips = sorted(glob.glob(zip_glob))
    if not zips:
        die(f"No ZIP matched: {zip_glob}")
    zip_path = zips[-1]  # latest by name
    print(f"[INFO] Using ZIP: {zip_path}")

    # 1) Upload file (purpose=assistants)
    files = {
        "file": (os.path.basename(zip_path), open(zip_path, "rb")),
    }
    data = {"purpose": "assistants"}
    r = requests.post(f"{API}/files",
                      headers={"Authorization": f"Bearer {api_key}"},
                      files=files, data=data, timeout=600)
    if r.status_code >= 400:
        die(f"Upload failed: {r.status_code} {r.text}")
    file_id = r.json()["id"]
    print(f"[INFO] Uploaded file_id={file_id}")

    # 2) Create or reuse thread, attach file via attachments (file_search tool)
    if not thread_id:
        payload = {
            "messages": [{
                "role": "user",
                "content": msg_text,
                "attachments": [
                    {"file_id": file_id, "tools": [{"type": "file_search"}]}
                ]
            }]
        }
        t = post_json(f"{API}/threads", payload)
        thread_id = t["id"]
        print(f"[INFO] Created thread_id={thread_id}")
    else:
        payload = {
            "role": "user",
            "content": msg_text,
            "attachments": [
                {"file_id": file_id, "tools": [{"type": "file_search"}]}
            ]
        }
        _ = post_json(f"{API}/threads/{thread_id}/messages", payload)
        print(f"[INFO] Posted message to thread_id={thread_id}")

    # 3) Kick off a Run with your Assistant
    run = post_json(f"{API}/threads/{thread_id}/runs",
                    {"assistant_id": asst_id,
                     "instructions": "添付ZIPを解析し、最新のファンダメンタルテーブルと注目銘柄（長期ロング）を作成してください。"})
    run_id = run["id"]
    print(f"[INFO] Run started run_id={run_id}")

    # 4) (Optional) Poll status (log only; do not fail workflow on WIP)
    for _ in range(30):
        time.sleep(2)
        r = requests.get(f"{API}/threads/{thread_id}/runs/{run_id}",
                         headers=HEADERS_JSON, timeout=60)
        if r.status_code >= 400:
            print(f"[WARN] Poll failed: {r.status_code} {r.text}")
            break
        st = r.json().get("status")
        print(f"[INFO] Run status: {st}")
        if st in ("completed", "failed", "cancelled", "expired"):
            break

    # Export IDs to logs (for easy reuse)
    print(f"THREAD_ID={thread_id}")
    print(f"RUN_ID={run_id}")

if __name__ == "__main__":
    main()
