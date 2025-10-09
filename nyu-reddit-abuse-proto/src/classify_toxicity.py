#!/usr/bin/env python3
"""
Toxicity Classification Pipeline
Uses unitary/unbiased-toxic-roberta to classify posts and comments
"""

import sqlite3
import re
import json
import sys
import os
from datetime import datetime
from typing import List, Dict, Tuple
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import pandas as pd

# Add src to path for imports
sys.path.append(os.path.dirname(__file__))

# Protected terms for identity attack detection
PROTECTED_TERMS = {
    "black", "asian", "white", "latino", "muslim", "jew", "jewish", "christian", "arab",
    "indian", "gay", "trans", "lgbt", "women", "female", "male", "immigrant", "chinese",
    "korean", "mexican", "african", "hispanic", "queer", "lesbian", "turk", "armenian"
}

def light_deobfuscate(s: str) -> str:
    """Light de-obfuscation for common symbol substitutions"""
    # Map common symbol -> letter; keeps most words intact
    table = str.maketrans({"$":"s","@":"a","!":"i","1":"l","0":"o","3":"e","*":"a"})
    return s.translate(table)

def get_parent_text(conn, full_raw_obj: dict):
    """Get parent text for context - CRITICAL: parent_id is at top-level JSON"""
    # parent_id is at top-level, not under raw_data
    parent_id = full_raw_obj.get('parent_id')  # e.g., 't1_xxx' or 't3_xxx'
    link_id = full_raw_obj.get('link_id')      # e.g., 't3_xxx' for thread root
    
    if not parent_id and not link_id:
        return ""

    def _fetch_comment_text(cid: str):
        row = conn.execute("SELECT raw_json FROM comments WHERE id=?", (cid,)).fetchone()
        if not row: return ""
        try:
            r = json.loads(row[0])
            return (r.get('raw_data', {}) or {}).get('body', '') or ''
        except: return ""

    def _fetch_post_text(pid: str):
        row = conn.execute("SELECT raw_json FROM posts WHERE id=?", (pid,)).fetchone()
        if not row: return ""
        try:
            r = json.loads(row[0])
            rd = r.get('raw_data', {}) or {}
            return ((rd.get('title','') or '') + ' ' + (rd.get('body','') or '')).strip()
        except: return ""

    base = lambda s: s.split('_',1)[-1] if s else None

    # Try direct parent first
    pid = base(parent_id)
    if pid:
        # parent could be a comment or a post
        txt = _fetch_comment_text(pid)
        if txt: return txt
        txt = _fetch_post_text(pid)
        if txt: return txt

    # Fallback: thread root
    lid = base(link_id)
    if lid:
        txt = _fetch_post_text(lid)
        if txt: return txt

    return ""

def clean_text(text: str) -> Tuple[str, Dict[str, bool]]:
    """
    Clean text for toxicity classification
    
    Returns:
        cleaned_text: Cleaned text ready for classification
        flags: Dictionary with deletion/removal flags
    """
    if not text or text.strip() == "":
        return "", {"is_deleted": False, "is_removed": False, "is_empty": True}
    
    # Check for deleted/removed content
    flags = {
        "is_deleted": text.strip() == "[deleted]",
        "is_removed": text.strip() == "[removed]",
        "is_empty": False
    }
    
    if flags["is_deleted"] or flags["is_removed"]:
        return text, flags
    
    # Core text cleaning
    cleaned = text
    
    # Remove URLs
    cleaned = re.sub(r"http\S+|www\S+|https\S+", "", cleaned)
    
    # Remove usernames & mentions
    cleaned = re.sub(r"u/[A-Za-z0-9_-]+", "<USER>", cleaned)
    cleaned = re.sub(r"@[A-Za-z0-9_-]+", "<USER>", cleaned)
    
    # Strip Markdown artifacts (basic)
    cleaned = re.sub(r"&[a-zA-Z]+;", "", cleaned)  # HTML entities
    cleaned = re.sub(r"&amp;", "&", cleaned)
    cleaned = re.sub(r"&lt;", "<", cleaned)
    cleaned = re.sub(r"&gt;", ">", cleaned)
    
    # Remove quote markers
    cleaned = re.sub(r"^>\s*", "", cleaned, flags=re.MULTILINE)
    
    # Normalize whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    
    # Lowercase
    cleaned = cleaned.lower()
    
    flags["is_empty"] = len(cleaned.strip()) == 0
    
    return cleaned, flags

def get_toxicity_thresholds():
    """Get toxicity classification thresholds - optimized for identity attack recall"""
    return {
        "toxic": {"high": 0.70, "medium": 0.50},
        "insult": {"high": 0.78, "medium": 0.55},          # keep ~0.78
        "identity_attack": {"high": 0.58, "medium": 0.38}, # lowered for better recall
        "threat": {"high": 0.68, "medium": 0.55},
        "sexual_explicit": {"high": 0.75, "medium": 0.55},
        "severe_toxic": {"high": 0.80, "medium": 0.60},
        "severe_toxicity": {"high": 0.80, "medium": 0.60}  # Alternative name
    }

def classify_toxicity_batch(texts: List[str], tokenizer, model) -> List[Dict[str, float]]:
    """
    Classify multiple texts for toxicity using batch processing
    
    Returns:
        List of dictionaries with toxicity scores for each label
    """
    if not texts:
        return []
    
    # Filter out empty texts and create mapping
    valid_texts = []
    text_mapping = []
    
    for i, text in enumerate(texts):
        if text and len(text.strip()) > 0:
            valid_texts.append(text)
            text_mapping.append(i)
        else:
            text_mapping.append(-1)  # Mark as empty
    
    if not valid_texts:
        # All texts were empty
        return [{model.config.id2label[i]: 0.0 for i in range(model.config.num_labels)} for _ in texts]
    
    # Batch tokenization with padding for stability
    inputs = tokenizer(valid_texts, truncation=True, padding=True, max_length=512, return_tensors="pt")
    
    with torch.no_grad():
        outputs = model(**inputs)
        logits = outputs.logits
        probabilities = torch.sigmoid(logits).cpu().numpy()
    
    # Map to label names using proper indexing
    labels = [model.config.id2label[i] for i in range(model.config.num_labels)]
    
    # Create results for all texts
    results = []
    valid_idx = 0
    
    for i in range(len(texts)):
        if text_mapping[i] == -1:
            # Empty text
            results.append({labels[j]: 0.0 for j in range(len(labels))})
        else:
            # Valid text
            scores = {labels[j]: float(probabilities[valid_idx][j]) for j in range(len(labels))}
            results.append(scores)
            valid_idx += 1
    
    return results

def classify_toxicity(text: str, tokenizer, model) -> Dict[str, float]:
    """
    Classify single text for toxicity using the model
    
    Returns:
        Dictionary with toxicity scores for each label
    """
    results = classify_toxicity_batch([text], tokenizer, model)
    return results[0] if results else {model.config.id2label[i]: 0.0 for i in range(model.config.num_labels)}

def update_db_schema(db_path: str):
    """Add classification results table to database"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create classification results table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS toxicity_classifications (
            id TEXT PRIMARY KEY,
            item_type TEXT CHECK(item_type IN ('post', 'comment')),
            text_cleaned TEXT,
            is_deleted INTEGER DEFAULT 0,
            is_removed INTEGER DEFAULT 0,
            is_empty INTEGER DEFAULT 0,
            toxic REAL DEFAULT 0.0,
            insult REAL DEFAULT 0.0,
            identity_attack REAL DEFAULT 0.0,
            threat REAL DEFAULT 0.0,
            sexual_explicit REAL DEFAULT 0.0,
            severe_toxic REAL DEFAULT 0.0,
            severe_toxicity REAL DEFAULT 0.0,
            classification_timestamp TEXT
        )
    """)
    
    # Create indexes for better performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_toxicity_toxic ON toxicity_classifications(toxic)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_toxicity_insult ON toxicity_classifications(insult)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_toxicity_identity ON toxicity_classifications(identity_attack)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_toxicity_type ON toxicity_classifications(item_type)")
    
    conn.commit()
    conn.close()
    print("✅ Updated database schema with toxicity classifications table")

def process_all_items(db_path: str, batch_size: int = 32):
    """Process all posts and comments for toxicity classification"""
    
    print("🚀 Starting toxicity classification pipeline...")
    
    # Load model
    print("📦 Loading toxicity model...")
    model_name = "unitary/unbiased-toxic-roberta"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    model.eval()
    
    print(f"✅ Model loaded: {model_name}")
    print("Model labels:", [model.config.id2label[i] for i in range(model.config.num_labels)])
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all posts with raw data
    cursor.execute("SELECT id, raw_json FROM posts")
    posts = cursor.fetchall()
    
    # Get all comments with raw data
    cursor.execute("SELECT id, raw_json FROM comments")
    comments = cursor.fetchall()
    
    print(f"📊 Processing {len(posts)} posts and {len(comments)} comments...")

    processed_count = 0
    flagged_count = 0
    thresholds = get_toxicity_thresholds()
    BATCH_SIZE = 64

    # Process posts
    for post_id, raw_json in posts:
        try:
            raw_data = json.loads(raw_json)
            title = raw_data.get('raw_data', {}).get('title', '')
            body = raw_data.get('raw_data', {}).get('body', '')
            # Combine title and body
            text = f"{title or ''} {body or ''}".strip()
        except:
            text = ""
        
        cleaned_text, flags = clean_text(text)
        
        if flags["is_deleted"] or flags["is_removed"] or flags["is_empty"]:
            # Store flags but skip classification
            cursor.execute("""
                INSERT OR REPLACE INTO toxicity_classifications 
                (id, item_type, text_cleaned, is_deleted, is_removed, is_empty, classification_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (post_id, "post", cleaned_text, flags["is_deleted"], flags["is_removed"], flags["is_empty"], datetime.now().isoformat()))
        else:
            # Apply light de-obfuscation before classification
            deobfuscated_text = light_deobfuscate(cleaned_text)
            # Classify toxicity
            scores = classify_toxicity(deobfuscated_text, tokenizer, model)
            
            # Check if flagged
            is_flagged = any(
                scores.get(label, 0) >= thresholds.get(label, {}).get("high", 0.7)
                for label in ["toxic", "insult", "identity_attack", "threat", "sexual_explicit", "severe_toxic", "severe_toxicity"]
            )
            
            # Log borderline identity attacks for review
            identity_score = scores.get("identity_attack", 0)
            if 0.30 <= identity_score < thresholds["identity_attack"]["high"]:
                print(f"[borderline identity] {post_id if 'post_id' in locals() else comment_id}: {identity_score:.2f} :: {cleaned_text[:160]}")
            
            # Telemetry: Check for protected terms with low identity scores
            txt_for_scan = cleaned_text.lower()
            if any(w in txt_for_scan for w in PROTECTED_TERMS):
                ia = scores.get("identity_attack", 0.0)
                if ia < thresholds["identity_attack"]["high"]:
                    print(f"[identity low but protected-term present] {post_id if 'post_id' in locals() else comment_id} "
                          f"IA={ia:.2f} :: {txt_for_scan[:180]}")
            
            if is_flagged:
                flagged_count += 1
            
            # Store results
            cursor.execute("""
                INSERT OR REPLACE INTO toxicity_classifications 
                (id, item_type, text_cleaned, is_deleted, is_removed, is_empty,
                 toxic, insult, identity_attack, threat, sexual_explicit, severe_toxic, severe_toxicity,
                 classification_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                post_id, "post", cleaned_text, flags["is_deleted"], flags["is_removed"], flags["is_empty"],
                scores.get("toxic", 0), scores.get("insult", 0), scores.get("identity_attack", 0),
                scores.get("threat", 0), scores.get("sexual_explicit", 0), scores.get("severe_toxic", 0),
                scores.get("severe_toxicity", 0), datetime.now().isoformat()
            ))
        
        processed_count += 1
        if processed_count % 50 == 0:
            print(f"   Processed {processed_count} items...")
    
    # Process comments in batches for better performance
    print("📝 Processing comments in batches...")
    comment_batch = []
    comment_data = []
    
    for comment_id, raw_json in comments:
        try:
            raw_data = json.loads(raw_json)
            body = raw_data.get('raw_data', {}).get('body', '')
            
            # Get parent context for better classification - pass full raw_data
            parent_text = get_parent_text(conn, raw_data)
            # CRITICAL: Child first, then parent - truncation drops parent first
            text = ((body or "") + "\n\nPARENT: " + (parent_text or "")).strip()
        except:
            text = ""
        
        cleaned_text, flags = clean_text(text)
        
        if flags["is_deleted"] or flags["is_removed"] or flags["is_empty"]:
            # Store flags but skip classification
            cursor.execute("""
                INSERT OR REPLACE INTO toxicity_classifications 
                (id, item_type, text_cleaned, is_deleted, is_removed, is_empty, classification_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (comment_id, "comment", cleaned_text, flags["is_deleted"], flags["is_removed"], flags["is_empty"], datetime.now().isoformat()))
        else:
            # Apply light de-obfuscation before classification
            deobfuscated_text = light_deobfuscate(cleaned_text)
            comment_batch.append(deobfuscated_text)
            comment_data.append((comment_id, cleaned_text, flags))
        
        processed_count += 1
        
        # Process batch when it reaches BATCH_SIZE
        if len(comment_batch) >= BATCH_SIZE:
            # Classify batch
            batch_scores = classify_toxicity_batch(comment_batch, tokenizer, model)
            
            # Store results
            for i, (comment_id, cleaned_text, flags) in enumerate(comment_data):
                scores = batch_scores[i]
                
                # Check if flagged
                is_flagged = any(
                    scores.get(label, 0) >= thresholds.get(label, {}).get("high", 0.7)
                    for label in ["toxic", "insult", "identity_attack", "threat", "sexual_explicit", "severe_toxic", "severe_toxicity"]
                )
                
                # Log borderline identity attacks for review
                identity_score = scores.get("identity_attack", 0)
                if 0.30 <= identity_score < thresholds["identity_attack"]["high"]:
                    print(f"[borderline identity] {comment_id}: {identity_score:.2f} :: {cleaned_text[:160]}")
                
                # Telemetry: Check for protected terms with low identity scores
                txt_for_scan = cleaned_text.lower()
                if any(w in txt_for_scan for w in PROTECTED_TERMS):
                    ia = scores.get("identity_attack", 0.0)
                    if ia < thresholds["identity_attack"]["high"]:
                        print(f"[identity low but protected-term present] {comment_id} "
                              f"IA={ia:.2f} :: {txt_for_scan[:180]}")
                
                if is_flagged:
                    flagged_count += 1
                
                # Store results
                cursor.execute("""
                    INSERT OR REPLACE INTO toxicity_classifications 
                    (id, item_type, text_cleaned, is_deleted, is_removed, is_empty,
                     toxic, insult, identity_attack, threat, sexual_explicit, severe_toxic, severe_toxicity,
                     classification_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    comment_id, "comment", cleaned_text, flags["is_deleted"], flags["is_removed"], flags["is_empty"],
                    scores.get("toxic", 0), scores.get("insult", 0), scores.get("identity_attack", 0),
                    scores.get("threat", 0), scores.get("sexual_explicit", 0), scores.get("severe_toxic", 0),
                    scores.get("severe_toxicity", 0), datetime.now().isoformat()
                ))
            
            # Clear batch
            comment_batch = []
            comment_data = []
        
        if processed_count % 50 == 0:
            print(f"   Processed {processed_count} items...")
    
    # Process remaining comments in final batch
    if comment_batch:
        batch_scores = classify_toxicity_batch(comment_batch, tokenizer, model)
        
        for i, (comment_id, cleaned_text, flags) in enumerate(comment_data):
            scores = batch_scores[i]
            
            # Check if flagged
            is_flagged = any(
                scores.get(label, 0) >= thresholds.get(label, {}).get("high", 0.7)
                for label in ["toxic", "insult", "identity_attack", "threat", "sexual_explicit", "severe_toxic", "severe_toxicity"]
            )
            
            # Log borderline identity attacks for review
            identity_score = scores.get("identity_attack", 0)
            if 0.30 <= identity_score < thresholds["identity_attack"]["high"]:
                print(f"[borderline identity] {comment_id}: {identity_score:.2f} :: {cleaned_text[:160]}")
            
            # Telemetry: Check for protected terms with low identity scores
            txt_for_scan = cleaned_text.lower()
            if any(w in txt_for_scan for w in PROTECTED_TERMS):
                ia = scores.get("identity_attack", 0.0)
                if ia < thresholds["identity_attack"]["high"]:
                    print(f"[identity low but protected-term present] {comment_id} "
                          f"IA={ia:.2f} :: {txt_for_scan[:180]}")
            
            if is_flagged:
                flagged_count += 1
            
            # Store results
            cursor.execute("""
                INSERT OR REPLACE INTO toxicity_classifications 
                (id, item_type, text_cleaned, is_deleted, is_removed, is_empty,
                 toxic, insult, identity_attack, threat, sexual_explicit, severe_toxic, severe_toxicity,
                 classification_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                comment_id, "comment", cleaned_text, flags["is_deleted"], flags["is_removed"], flags["is_empty"],
                scores.get("toxic", 0), scores.get("insult", 0), scores.get("identity_attack", 0),
                scores.get("threat", 0), scores.get("sexual_explicit", 0), scores.get("severe_toxic", 0),
                scores.get("severe_toxicity", 0), datetime.now().isoformat()
            ))
    
    conn.commit()
    conn.close()
    
    print(f"\n🎉 Classification complete!")
    print(f"   Total processed: {processed_count}")
    print(f"   Flagged items: {flagged_count}")
    print(f"   Flagged rate: {flagged_count/processed_count*100:.1f}%")

def main():
    """Main function"""
    db_path = "nyu_reddit_local.sqlite"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        print("Make sure you have the local database file.")
        return
    
    # Update database schema
    update_db_schema(db_path)
    
    # Process all items
    process_all_items(db_path)

if __name__ == "__main__":
    main()
