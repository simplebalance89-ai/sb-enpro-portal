"""
Enpro Voice Echo — Predictive Pre-Fetch System
Grades accuracy, echoes predictions in background, learns patterns.
"""

import asyncio
import json
import re
import time
from typing import List, Dict, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import threading
import queue

@dataclass
class EchoResult:
    """Single echo (prediction)."""
    source_query: str
    predicted_query: str
    confidence: float  # How likely this is the next question
    products: List[dict]
    latency_ms: float
    timestamp: str

@dataclass
class AccuracyGrade:
    """How accurate the response was."""
    query: str
    accuracy_pct: float  # 0-100%
    match_type: str  # 'exact', 'fuzzy', 'keyword', 'none'
    products_found: int
    response_time_ms: float


class VoiceEcho:
    """
    Predictive echo system with DEFERRED RESPONSES.
    
    FLOW:
    1. User query -> Grade accuracy -> Return response
    2. BACKGROUND: Echo thread starts pre-fetching:
       - Crosswalk equivalents
       - Related applications  
       - What user typically asks next (learned)
    3. Next query -> Likely already cached -> instant
    4. Learn: Store pattern (query A -> usually query B)
    
    DEFERRED MODE:
    - User asks for deep info (specs, manufacturer, crosswalk)
    - Immediate: "Give me a second while I look that up"
    - 10-15s later: Echo returns with full details
    """
    
    def __init__(self, voice_gate, delay_seconds: float = 0, defer_seconds: float = 12):
        self.gate = voice_gate
        self.delay_seconds = delay_seconds
        self.defer_seconds = defer_seconds  # Time for deep lookups
        
        # Learning database
        self.patterns: Dict[str, Dict[str, int]] = {}  # query -> {next_query: count}
        self.echo_cache: Dict[str, EchoResult] = {}  # predicted_query -> result
        self.accuracy_history: List[AccuracyGrade] = []
        
        # Deferred responses
        self.deferred_callbacks: Dict[str, Callable] = {}  # query -> callback
        
        # Background echo queue
        self.echo_queue = queue.Queue()
        self.echo_thread = threading.Thread(target=self._echo_worker, daemon=True)
        self.echo_thread.start()
        
        # Deferred response thread
        self.defer_thread = threading.Thread(target=self._defer_worker, daemon=True)
        self.defer_thread.start()
        
        # Load learned patterns
        self._load_patterns()
    
    def query(self, user_query: str, wait_for_echo: bool = False, 
              defer: bool = False, on_deferred: Callable = None) -> tuple[str, AccuracyGrade]:
        """
        Main entry: grade accuracy, return response, trigger background echo.
        
        Args:
            user_query: What user said
            wait_for_echo: If True, wait delay_seconds for echo to warm up
            defer: If True, return "looking it up" and echo back later
            on_deferred: Callback for deferred response
            
        Returns:
            (response_text, accuracy_grade)
        """
        # Check if this is a DEEP query (specs, manufacturer, crosswalk)
        is_deep_query = self._is_deep_query(user_query)
        
        if is_deep_query and defer:
            # DEFERRED: Queue for deep lookup, return immediately
            self._queue_deferred(user_query, on_deferred)
            grade = AccuracyGrade(
                query=user_query,
                accuracy_pct=0.0,  # Pending
                match_type='deferred',
                products_found=0,
                response_time_ms=0
            )
            return f"Give me a second while I look that up...", grade
        
        start_time = time.time()
        
        # 1. Check cache first (instant if echoed)
        cached = self.echo_cache.get(user_query.lower())
        if cached:
            response_time = (time.time() - start_time) * 1000
            grade = AccuracyGrade(
                query=user_query,
                accuracy_pct=95.0,  # Cached = high accuracy
                match_type='echo_cached',
                products_found=len(cached.products),
                response_time_ms=response_time
            )
            self.accuracy_history.append(grade)
            return self._format_cached(cached), grade
        
        # 2. Fresh lookup
        result = self.gate.lookup(user_query)
        response_time = (time.time() - start_time) * 1000
        
        # 3. Grade accuracy
        grade = self._grade_accuracy(user_query, result, response_time)
        self.accuracy_history.append(grade)
        if len(self.accuracy_history) > 1000:
            self.accuracy_history = self.accuracy_history[-500:]
        
        # 4. Format response
        response = self._format_response(result, grade)
        
        # 5. Trigger background echo (predict next queries)
        self._trigger_echo(user_query, result)
        
        # 6. Optional delay for echo warm-up
        if wait_for_echo and self.delay_seconds > 0:
            time.sleep(self.delay_seconds)
        
        return response, grade
    
    def _is_deep_query(self, query: str) -> bool:
        """Check if query needs deep lookup (specs, manufacturer, crosswalk)."""
        deep_keywords = [
            'spec', 'specification', 'detail', 'manufacturer', 'crosswalk',
            'equivalent', 'replaces', 'compatible', 'material', 'micron',
            'pressure', 'temp', 'temperature', 'flow', 'media'
        ]
        return any(kw in query.lower() for kw in deep_keywords)
    
    def _queue_deferred(self, query: str, callback: Callable = None):
        """Queue a deferred query for background processing."""
        self.deferred_callbacks[query.lower()] = callback or (lambda x: print(f"\n[ECHO] {x}"))
        
        # Queue it
        self.echo_queue.put({
            'source_query': query,
            'predicted_query': query,
            'confidence': 1.0,
            'deferred': True
        })
    
    def _defer_worker(self):
        """Background thread: handles deferred responses."""
        while True:
            time.sleep(1)
            
            # Check for completed deferred lookups
            completed = []
            for query_lower, callback in list(self.deferred_callbacks.items()):
                cached = self.echo_cache.get(query_lower)
                if cached:
                    # Found! Trigger callback
                    response = self._format_cached(cached)
                    try:
                        callback(response)
                    except:
                        print(f"\n[ECHO] {response}")
                    completed.append(query_lower)
            
            # Clean up completed
            for q in completed:
                del self.deferred_callbacks[q]
    
    def next_echo(self, user_query: str) -> Optional[str]:
        """
        User said 'next' or 'what else' — return next best echo.
        """
        # Find echoes for this query pattern
        echoes = [e for e in self.echo_cache.values() 
                  if e.source_query.lower() == user_query.lower()]
        
        if not echoes:
            return "No predictions ready. What else are you looking for?"
        
        # Sort by confidence
        echoes.sort(key=lambda x: x.confidence, reverse=True)
        
        # Return highest confidence not yet shown
        for echo in echoes:
            return self._format_cached(echo)
        
        return None
    
    def learn(self, query_a: str, query_b: str):
        """
        Learn: After query A, user usually asks query B.
        Called when user makes a second query.
        """
        q_a = query_a.lower()
        q_b = query_b.lower()
        
        if q_a not in self.patterns:
            self.patterns[q_a] = {}
        
        self.patterns[q_a][q_b] = self.patterns[q_a].get(q_b, 0) + 1
        
        # Save patterns
        self._save_patterns()
    
    def get_stats(self) -> dict:
        """Get echo system stats."""
        if not self.accuracy_history:
            return {"status": "No queries yet"}
        
        avg_accuracy = sum(a.accuracy_pct for a in self.accuracy_history) / len(self.accuracy_history)
        avg_latency = sum(a.response_time_ms for a in self.accuracy_history) / len(self.accuracy_history)
        
        return {
            "total_queries": len(self.accuracy_history),
            "avg_accuracy_pct": round(avg_accuracy, 1),
            "avg_latency_ms": round(avg_latency, 1),
            "patterns_learned": len(self.patterns),
            "echo_cache_size": len(self.echo_cache)
        }
    
    # === INTERNAL ===
    
    def _grade_accuracy(self, query: str, result, response_time_ms: float) -> AccuracyGrade:
        """Grade how accurate the response is."""
        if not result.found:
            return AccuracyGrade(
                query=query,
                accuracy_pct=0.0,
                match_type='none',
                products_found=0,
                response_time_ms=response_time_ms
            )
        
        # Exact match = 100%
        if result.alt_code and result.alt_code.lower() == query.lower():
            return AccuracyGrade(
                query=query,
                accuracy_pct=100.0,
                match_type='exact',
                products_found=1,
                response_time_ms=response_time_ms
            )
        
        # Part number match = 95%
        if result.part_number and result.part_number.lower() == query.lower():
            return AccuracyGrade(
                query=query,
                accuracy_pct=95.0,
                match_type='exact_part',
                products_found=1,
                response_time_ms=response_time_ms
            )
        
        # Fuzzy match — map string confidence to numeric score
        if result.match_confidence:
            confidence_map = {
                'exact': 95.0,
                'fuzzy': 80.0,
                'criteria_match': 70.0,
                'none': 50.0,
            }
            pct = confidence_map.get(result.match_confidence, 75.0)
            return AccuracyGrade(
                query=query,
                accuracy_pct=pct,
                match_type='fuzzy',
                products_found=1,
                response_time_ms=response_time_ms
            )
        
        # Keyword match = 50%
        return AccuracyGrade(
            query=query,
            accuracy_pct=50.0,
            match_type='keyword',
            products_found=1,
            response_time_ms=response_time_ms
        )
    
    def _trigger_echo(self, source_query: str, result):
        """Queue up background echo predictions."""
        predictions = []
        
        # 1. Crosswalk predictions (if part found)
        if result.found:
            predictions.append({
                'predicted': f"crosswalk {result.alt_code or result.part_number}",
                'confidence': 0.85
            })
            predictions.append({
                'predicted': f"{result.manufacturer} equivalent",
                'confidence': 0.75
            })
        
        # 2. Application predictions
        if result.found and result.application:
            predictions.append({
                'predicted': f"{result.application} filters",
                'confidence': 0.80
            })
        
        # 3. Learned patterns
        learned = self.patterns.get(source_query.lower(), {})
        for next_q, count in sorted(learned.items(), key=lambda x: -x[1])[:3]:
            predictions.append({
                'predicted': next_q,
                'confidence': min(0.90, 0.50 + (count * 0.10))
            })
        
        # 4. Common follow-ups
        predictions.extend([
            {'predicted': 'price', 'confidence': 0.60},
            {'predicted': 'stock', 'confidence': 0.55},
            {'predicted': 'alternative', 'confidence': 0.50},
        ])
        
        # Queue for background processing
        for pred in predictions:
            self.echo_queue.put({
                'source_query': source_query,
                'predicted_query': pred['predicted'],
                'confidence': pred['confidence']
            })
    
    def _echo_worker(self):
        """Background thread: processes echo queue."""
        while True:
            try:
                task = self.echo_queue.get(timeout=1)
                if task is None:
                    break
                
                start = time.time()
                
                # Do the prediction lookup
                result = self.gate.lookup(task['predicted_query'])
                
                # Store in cache
                echo = EchoResult(
                    source_query=task['source_query'],
                    predicted_query=task['predicted_query'],
                    confidence=task['confidence'],
                    products=[self._to_dict(result)] if result.found else [],
                    latency_ms=(time.time() - start) * 1000,
                    timestamp=datetime.utcnow().isoformat()
                )
                
                self.echo_cache[task['predicted_query'].lower()] = echo
                # Cap cache at 500 entries
                if len(self.echo_cache) > 500:
                    oldest_keys = list(self.echo_cache.keys())[:100]
                    for k in oldest_keys:
                        del self.echo_cache[k]

            except queue.Empty:
                continue
            except Exception as e:
                print(f"Echo error: {e}")
    
    def _format_response(self, result, grade: AccuracyGrade) -> str:
        """Format main response."""
        if not result.found:
            return f"Not found (accuracy: 0%). Try a part number."
        
        lines = [
            f"Found {result.alt_code or result.part_number}",
            f"Accuracy: {grade.accuracy_pct:.0f}% | Type: {grade.match_type}",
        ]
        
        if result.description:
            lines.append(result.description[:60])
        
        if result.price and result.price > 0:
            lines.append(f"Price: ${result.price:.2f}")
        
        stock_str = "unknown"
        if result.in_stock is True:
            stock_str = f"{result.qty_on_hand} in stock"
        elif result.in_stock is False:
            stock_str = "out of stock"
        lines.append(f"Stock: {stock_str}")
        
        return " | ".join(lines)
    
    def _format_cached(self, echo: EchoResult) -> str:
        """Format cached echo response."""
        if not echo.products:
            return f"[Echo] {echo.predicted_query}: No results"
        
        p = echo.products[0]
        return f"[Echo] {echo.predicted_query} ({echo.confidence:.0%}): {p.get('alt_code', 'N/A')} — {p.get('description', 'N/A')[:40]}"
    
    def _to_dict(self, result) -> dict:
        """Convert to dict."""
        return {
            'alt_code': result.alt_code,
            'part_number': result.part_number,
            'description': result.description,
            'manufacturer': result.manufacturer,
            'application': result.application,
            'price': result.price,
            'in_stock': result.in_stock,
            'qty': result.qty_on_hand,
        }
    
    def _load_patterns(self):
        """Load learned patterns from disk."""
        path = Path("data/voice_echo_patterns.json")
        if path.exists():
            with open(path) as f:
                self.patterns = json.load(f)
    
    def _save_patterns(self):
        """Save learned patterns."""
        path = Path("data/voice_echo_patterns.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w') as f:
            json.dump(self.patterns, f, indent=2)


# === DEMO ===

def run_demo():
    """Simple demo."""
    from voice_gate import VoiceGate
    import pandas as pd
    
    # Create mock data
    df = pd.DataFrame([
        {'Alt_Code': 'HC9600', 'Part_Number': 'PN001', 'Description': 'Pall Hydraulic Filter', 
         'Manufacturer': 'Pall', 'Application': 'Hydraulic Oil', 'Price': 45.99, 'In_Stock': True, 'Qty_On_Hand': 10},
        {'Alt_Code': 'CLR10295', 'Part_Number': 'PN002', 'Description': 'Compressed Air Filter',
         'Manufacturer': 'Other', 'Application': 'Compressed Air', 'Price': 32.50, 'In_Stock': None},
        {'Alt_Code': 'HCA123', 'Part_Number': 'PN003', 'Description': 'Hydraulic Filter 10 micron',
         'Manufacturer': 'Pall', 'Application': 'Hydraulic Oil', 'Price': 55.00, 'In_Stock': True, 'Qty_On_Hand': 5},
    ])
    
    data_path = "/tmp/mock_filtration.csv"
    df.to_csv(data_path, index=False)
    
    gate = VoiceGate(data_path)
    echo = VoiceEcho(gate, delay_seconds=0, defer_seconds=3)  # 3s defer for demo
    
    print("=" * 60)
    print("VOICE ECHO - Predictive Pre-Fetch with DEFERRED RESPONSES")
    print("=" * 60)
    print("\nTry these:")
    print("  HC9600              -> instant lookup")
    print("  manufacturer HC9600 -> 'Give me a second...' then echo")
    print("  specs HC9600        -> 'Give me a second...' then echo")
    print("  stats               -> system stats")
    print("  quit                -> exit")
    print("-" * 60)
    
    last_query = None
    
    while True:
        try:
            query = input("\n> ").strip()
            
            if not query:
                continue
            
            if query == 'quit':
                break
            
            if query == 'stats':
                print(json.dumps(echo.get_stats(), indent=2))
                continue
            
            if query == 'cache':
                print("Echo cache:")
                for k, v in echo.echo_cache.items():
                    print(f"  {k}: {v.confidence:.0%} confidence")
                continue
            
            if query == 'patterns':
                print("Learned patterns:")
                for src, dsts in echo.patterns.items():
                    print(f"  '{src}' -> {dsts}")
                continue
            
            # Check if deep query
            is_deep = echo._is_deep_query(query)
            
            # Process query
            response, grade = echo.query(query, defer=is_deep)
            print(f"\n{response}")
            
            if is_deep and grade.match_type == 'deferred':
                print("[Echo will return in ~3 seconds...]")
            
            # Learn from transition
            if last_query:
                echo.learn(last_query, query)
                print(f"[Learned: '{last_query}' -> '{query}']")
            
            last_query = query
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")
    
    print("\nGoodbye!")


if __name__ == "__main__":
    run_demo()
