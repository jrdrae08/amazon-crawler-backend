import asyncio
import json
import random
import re
import requests
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"status": "online"}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

def extract_asin(url: str) -> str:
    match = re.search(r'(?:/dp/|/gp/product/)([A-Z0-9]{10})', url, re.IGNORECASE)
    return match.group(1) if match else "N/A"

def parse_proxy(raw_proxy: str) -> str:
    """
    Converts IP:PORT:USER:PASS or IP:PORT into standard http://USER:PASS@IP:PORT syntax.
    """
    raw_proxy = raw_proxy.strip()
    if raw_proxy.startswith("http://") or raw_proxy.startswith("https://"):
        return raw_proxy
    
    parts = raw_proxy.split(":")
    if len(parts) == 4:
        ip, port, user, password = parts
        return f"http://{user}:{password}@{ip}:{port}"
    elif len(parts) == 2:
        ip, port = parts
        return f"http://{ip}:{port}"
    
    return raw_proxy

@app.websocket("/ws/crawler")
async def crawler_endpoint(websocket: WebSocket):
    await websocket.accept()
    
    crawler_task = None
    stop_requested = False

    async def run_crawler_loop(products, raw_proxies, max_cycles, start_cycle=1):
        nonlocal stop_requested

        # Format proxies to standard HTTP format
        proxies = [parse_proxy(p) for p in raw_proxies if p.strip()]

        try:
            # Loop starting from start_cycle up to max_cycles
            for cycle_count in range(start_cycle, max_cycles + 1):
                if stop_requested:
                    break

                # Send explicit cycle_update message to update UI cycle counter state
                await websocket.send_json({
                    "type": "cycle_update",
                    "current_cycle": cycle_count,
                    "max_cycles": max_cycles
                })

                await websocket.send_json({
                    "type": "log",
                    "message": f"=== Starting Cycle Round #{cycle_count} of {max_cycles} ===",
                    "logType": "header"
                })

                for index, product in enumerate(products, start=1):
                    if stop_requested:
                        break

                    current_proxy = random.choice(proxies)
                    proxy_dict = {"http": current_proxy, "https": current_proxy}
                    host_info = current_proxy.split('@')[-1] if '@' in current_proxy else current_proxy

                    await websocket.send_json({"type": "proxy_update", "proxy": current_proxy})
                    await websocket.send_json({
                        "type": "log",
                        "message": f"[{index}/{len(products)}] Requesting: '{product['name']}' ({product['asin']}) via Proxy: {host_info}",
                        "logType": "info"
                    })

                    try:
                        response = await asyncio.to_thread(
                            requests.get, product["url"], headers=HEADERS, proxies=proxy_dict, timeout=10
                        )

                        await websocket.send_json({
                            "type": "log",
                            "message": f"    Status: {response.status_code} | Bytes: {len(response.content)}",
                            "logType": "success" if response.status_code == 200 else "warning"
                        })
                    except Exception as err:
                        await websocket.send_json({
                            "type": "log",
                            "message": f"    Failed: {str(err)}",
                            "logType": "error"
                        })

                    await asyncio.sleep(random.uniform(2.0, 4.0))

                if stop_requested:
                    break

                # If not on the last cycle, pause before the next round
                if cycle_count < max_cycles:
                    await websocket.send_json({
                        "type": "log",
                        "message": f"=== Completed Round #{cycle_count}. Waiting before next round... ===",
                        "logType": "header"
                    })
                    await asyncio.sleep(8)

            # Log natural completion message
            if not stop_requested:
                await websocket.send_json({
                    "type": "log",
                    "message": f"SUCCESS: Completed all {max_cycles} cycle(s) successfully!",
                    "logType": "success"
                })

        except asyncio.CancelledError:
            pass
        finally:
            # Notify frontend that execution has ended so UI switches back to "Start" state
            try:
                await websocket.send_json({"type": "status", "status": "stopped"})
            except Exception:
                pass

    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)

            if message.get("action") == "start":
                stop_requested = False
                if crawler_task and not crawler_task.done():
                    crawler_task.cancel()

                raw_products = message.get("products", [])
                proxies = message.get("proxies", [])
                max_cycles = int(message.get("max_cycles", 10))
                
                # Extract start_cycle sent by frontend (defaults to 1 if not provided)
                start_cycle = int(message.get("start_cycle", 1))

                products = [
                    {**p, "asin": extract_asin(p["url"])} 
                    for p in raw_products if p.get("url")
                ]

                if products and proxies:
                    crawler_task = asyncio.create_task(
                        run_crawler_loop(products, proxies, max_cycles, start_cycle)
                    )

            elif message.get("action") == "stop":
                stop_requested = True
                if crawler_task:
                    crawler_task.cancel()
                await websocket.send_json({
                    "type": "log",
                    "message": "Execution stopped by user.",
                    "logType": "warning"
                })

    except WebSocketDisconnect:
        stop_requested = True
        if crawler_task:
            crawler_task.cancel()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="127.0.0.1", port=8000, reload=True)
