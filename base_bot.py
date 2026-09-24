import json
import sys
from dataclasses import asdict
from datetime import datetime
from functools import cached_property
from threading import Thread
from time import sleep, time
from typing import Any, Callable
from abc import ABC, abstractmethod
from traceback import format_exc

import requests
import sseclient

from models import Order, OrderBook, OrderRequest, OrderResponse, Product, Trade


def check_if_right_sse_used():
    # I don't have a better way to determine whether it is sseclient or sseclient-py.
    if "maxime.petazzoni" in getattr(sseclient, "__email__", ""):
        return

    print(
        """
It looks like you have installed the wrong SSE client library.
Please ensure you have installed `sseclient-py` and not `sseclient`.

To fix this, follow these steps:

1. If you have installed `sseclient`, uninstall it:
    ```
    pip uninstall sseclient
    ```

2. Install `sseclient-py`:
    ```
    pip install sseclient-py
    ```

To avoid such issues, it's recommended to use a virtual environment and install dependencies from `requirements.txt`.

Here's how you can set up a virtual environment and install the correct dependencies:

```
python3 -m venv .
source ./bin/activate
pip3 install -r ./requirements.txt
```
"""
    )
    quit(1)


check_if_right_sse_used()


STANDARD_HEADERS = {"Content-Type": "application/json; charset=utf-8"}
SSE_STOP_WAIT_SECS = 2
# No request may wait forever: a connection that dies without a close would otherwise hang the
# bot inside its handler with no output and no error.
REQUEST_TIMEOUT_SECS = 10
# A bot that hears nothing for this long looks at the exchange and says what it found.
STALL_SECS = 30
# The exchange keeps one stream per login and closes the old one when a new one connects, so a
# stream that keeps being closed means another bot is using the same login.
STREAM_CLOSES_THAT_MEAN_A_SHARED_LOGIN = 3
# Jupyter files a background thread's output under the cell that first started a thread with
# that id, so after a rerun in the same kernel the handler's prints go to a cell that no longer
# shows them. Only a kernel restart clears that; the bot says so instead of leaving a mystery.
_bots_started_in_this_process = 0


class SSEThread(Thread):
    bearer: str
    url: str
    _handle_orderbook: Callable[[OrderBook], Any]
    _handle_trade_event: Callable[[Trade], Any]
    _http_stream: requests.Response | None = None
    _client: sseclient.SSEClient | None = None
    _closed: bool = False
    last_event_at: float = 0.0
    events: int = 0
    _connected_once: bool = False
    _shared_login_warned_at: float = 0.0
    _closed_by_exchange_at: list[float]

    def __init__(
        self,
        bearer: str,
        url: str,
        handle_orderbook: Callable[[OrderBook], Any],
        handle_trade_event: Callable[[Trade], Any],
        say: Callable[[str], None] = print,
    ):
        # A daemon thread: a read that outlives stop() must not keep the program alive.
        super().__init__(daemon=True)

        self.bearer = bearer
        self.url = url
        self._handle_orderbook = handle_orderbook
        self._handle_trade_event = handle_trade_event
        self._closed_by_exchange_at = []
        self._say = say

    def run(self):
        while not self._closed:
            try:
                self._start_sse_client()
            except requests.RequestException as e:
                if not self._closed:
                    self._say(
                        f"Stream lost ({type(e).__name__}: {str(e)[:120]}); reconnecting"
                    )
                    sleep(1)
                continue
            except Exception:
                if not self._closed:
                    self._say(
                        "Your handler raised an error; the stream is being restarted:"
                    )
                    print(format_exc())
                    sleep(1)
                continue
            if self._closed:
                return
            self._explain_close_by_exchange()
            sleep(1)

    def _explain_close_by_exchange(self):
        now = time()
        self._closed_by_exchange_at = [
            t for t in self._closed_by_exchange_at if now - t < 60
        ] + [now]
        if not self._fighting_for_the_stream():
            self._say("The exchange closed the stream; reconnecting")
        elif now - self._shared_login_warned_at >= 60:
            self._shared_login_warned_at = now
            self._say(
                "The exchange has closed this stream three times in a minute. It keeps one stream "
                "per login, so another bot is almost certainly running with this username. Stop it, "
                "or use a different username here; until then this bot will miss most updates."
            )

    def _fighting_for_the_stream(self) -> bool:
        return (
            len(self._closed_by_exchange_at) >= STREAM_CLOSES_THAT_MEAN_A_SHARED_LOGIN
        )

    def close(self):
        self._closed = True
        if self._http_stream:
            self._http_stream.close()
        if self._client:
            self._client.close()

    def _handle_orderbook_change(self, orderbook: dict[str, Any]):
        buy_orders = sorted(
            [
                {
                    "price": float(price),
                    "volume": volumes["marketVolume"],
                    "own_volume": volumes["userVolume"],
                }
                for price, volumes in orderbook["buyOrders"].items()
            ],
            key=lambda d: -d["price"],
        )
        sell_orders = sorted(
            [
                {
                    "price": float(price),
                    "volume": volumes["marketVolume"],
                    "own_volume": volumes["userVolume"],
                }
                for price, volumes in orderbook["sellOrders"].items()
            ],
            key=lambda d: d["price"],
        )

        self._handle_orderbook(
            OrderBook(
                orderbook["productsymbol"],
                orderbook["tickSize"],
                list(map(lambda order: Order(**order), buy_orders)),
                list(map(lambda order: Order(**order), sell_orders)),
            )
        )

    def _start_sse_client(self):
        headers = {
            "Authorization": self.bearer,
            "Accept": "text/event-stream; charset=utf-8",
        }

        self._http_stream = requests.get(
            self.url, stream=True, headers=headers, timeout=30
        )
        if self._http_stream.status_code != 200:
            self._say(
                f"The exchange refused the stream (HTTP {self._http_stream.status_code}). If it says "
                "unauthorised, this login no longer exists on the exchange: sign up again and rerun."
            )
            sleep(5)
            return
        self.last_event_at = time()
        if not self._connected_once:
            self._say("Stream connected; waiting for the first update")
        elif not self._fighting_for_the_stream():
            self._say("Stream reconnected")
        self._connected_once = True
        self._client = sseclient.SSEClient(self._http_stream)

        for event in self._client.events():
            if self._closed:
                return
            self.last_event_at = time()
            self.events += 1
            if event.event == "order":
                self._handle_orderbook_change(json.loads(event.data))
            elif event.event == "trade":
                for trade in json.loads(event.data):
                    self._handle_trade_event(Trade(**trade))


class BaseBot(ABC):
    username: str
    _password: str
    _cmi_url: str
    _sse_thread: SSEThread | None

    def __init__(self, cmi_url: str, username: str, password: str):
        self._cmi_url = cmi_url
        self.username = username
        self._password = password
        self._sse_thread = None
        # One connection pool per bot. A fresh connection per request costs two extra round
        # trips (TCP and TLS) on every order, and a bot sending hundreds of orders a second
        # runs the machine out of local ports within a minute.
        self._http = requests.Session()
        self._orders_sent = 0
        self._orders_filled = 0
        self._failed_requests = 0
        self._last_note = ""

        self._register_bot()

    @cached_property
    def auth_token(self):
        return self._authenticate()

    def _register_bot(self) -> None:
        """
        Registers the user if they are not already registered.
        """
        all_users = self._http.get(
            f"{self._cmi_url}/api/user",
            timeout=REQUEST_TIMEOUT_SECS,
        )

        all_users.raise_for_status()

        if self.username in [user["username"] for user in all_users.json()]:
            print("Bot is already registered")
        else:
            self._register()

    def start(
        self,
        on_orderbook: Callable[[OrderBook], Any] | None = None,
        on_trades: Callable[[list[Trade]], Any] | None = None,
    ) -> None:
        """
        Creates an SSE thread to handle market events.
        """
        if self._sse_thread:
            raise Exception(
                "Bot is already running; call `stop()` before starting again"
            )
        if self._is_a_rerun_in_a_notebook():
            return

        self._sse_thread = SSEThread(
            bearer=self.auth_token,
            url=f"{self._cmi_url}/api/market/stream",
            handle_orderbook=on_orderbook or self.on_orderbook,
            handle_trade_event=lambda trade: (on_trades or self.on_trades)([trade]),
            say=self._say,
        )

        self._print_connection_summary()
        self._sse_thread.start()
        global _bots_started_in_this_process
        _bots_started_in_this_process += 1
        Thread(
            target=self._watch_for_silence, args=(self._sse_thread,), daemon=True
        ).start()

    def _is_a_rerun_in_a_notebook(self) -> bool:
        if not (_bots_started_in_this_process and "ipykernel" in sys.modules):
            return False
        print(
            "RESTART THE KERNEL. A bot has already run in this kernel, and after a rerun the "
            "notebook hides everything your bot prints, so this bot has NOT been started. "
            "Choose Kernel > Restart (or Restart & Run All), then run the cells from the top."
        )
        return True

    def wait(self, heartbeat_secs: float = 10) -> None:
        """
        Blocks until the cell is interrupted, printing a status line from the main thread every
        few seconds. Some notebooks stop showing output from background threads, and everything
        else this bot prints comes from one; this line always shows.
        """
        try:
            while self._sse_thread:
                sleep(heartbeat_secs)
                print(self.status_line())
        except KeyboardInterrupt:
            self.stop()

    def status_line(self) -> str:
        stream = self._sse_thread
        if stream is None:
            return "stopped"
        if not stream._connected_once:
            state = "stream not connected yet"
        elif stream.last_event_at:
            state = f"{stream.events} updates, last {time() - stream.last_event_at:.0f}s ago"
        else:
            state = "stream up, no update yet"
        line = (
            f"{datetime.now():%H:%M:%S}  {state}; {self._orders_sent} orders sent, "
            f"{self._orders_filled} filled; {self._failed_requests} failed requests"
        )
        if self._last_note:
            line += f"; last note: {self._last_note}"
        return line

    def _say(self, message: str) -> None:
        self._last_note = message
        print(message)

    def _print_connection_summary(self) -> None:
        status = self._get_json("/api/status")
        if status is None:
            self._say(
                f"Could not read the exchange status at {self._cmi_url}; starting anyway"
            )
            return
        trading = (
            "accepting orders"
            if status.get("acceptingOrders")
            else "NOT accepting orders (trading is stopped)"
        )
        limits = [
            limit.get("longLimit")
            for limit in status.get("positionLimits") or []
            if limit.get("longLimit")
        ]
        fees = "no position fees"
        if status.get("positionFee") is not None:
            fees = f"fees {status.get('positionFee')} a lot"
            if limits:
                fees += (
                    f", {status.get('excessPositionFee')} a lot beyond {min(limits)}"
                )
        print(
            f"Connected to {self._cmi_url} as {self.username}: round "
            f"{status.get('activeRoundName')}, {trading}; {fees}; "
            f"positions {self.request_positions() or {}}"
        )

    def _watch_for_silence(self, stream: SSEThread) -> None:
        started, warned = time(), False
        while self._sse_thread is stream and not stream._closed:
            sleep(5)
            quiet = time() - max(stream.last_event_at, started)
            if quiet >= STALL_SECS and not warned:
                warned = True
                self._say(self._explain_silence(quiet))
            elif quiet < STALL_SECS and warned:
                warned = False
                self._say("Updates are flowing again")

    def _explain_silence(self, quiet: float) -> str:
        head = f"No update for {quiet:.0f}s"
        status = self._get_json("/api/status")
        if status is None:
            return f"{head} and the exchange at {self._cmi_url} is not answering; check the address and your network"
        if not status.get("acceptingOrders"):
            return f"{head}: the exchange is not accepting orders (trading is stopped). Nothing to fix; updates resume when trading starts"
        products = self.request_all_products() or []
        books = [
            self._get_json(f"/api/product/{product.symbol}/order-book/current-user")
            for product in products[:3]
        ]
        if any(book and (book.get("buy") or book.get("sell")) for book in books):
            return (
                f"{head} although the market is live. Most likely another bot is logged in as "
                f"'{self.username}' (the exchange allows one stream per login), or this stream is "
                "stuck. Stop any other bot on this login, then interrupt and rerun the cell"
            )
        return f"{head}: the books are empty, so there is nothing to update. Nothing to fix"

    def _get_json(self, path: str) -> Any | None:
        response = self._request("GET", path)
        if response is None or response.status_code != 200:
            return None
        return response.json()

    def _request(self, method: str, path: str, **kwargs) -> requests.Response | None:
        """One request to the exchange; a failure is reported in one line and returns None."""
        try:
            response = self._http.request(
                method,
                f"{self._cmi_url}{path}",
                headers=self._get_headers(),
                timeout=REQUEST_TIMEOUT_SECS,
                **kwargs,
            )
        except requests.RequestException as e:
            self._failed_requests += 1
            self._say(f"{method} {path} failed: {type(e).__name__}: {str(e)[:150]}")
            return None
        if response.status_code in (401, 403):
            self._say(
                f"{method} {path} was refused (HTTP {response.status_code}): this login is no "
                "longer valid on the exchange. Sign up again and rerun the cell"
            )
        return response

    def stop(self) -> None:
        """
        Closes the SSE thread.
        """
        if not self._sse_thread:
            return
        print("Stopping")
        self._sse_thread.close()
        # Closing the stream does not interrupt a read already in progress. The thread wakes on
        # the next event or on its read timeout, handles nothing more, and exits on its own.
        self._sse_thread.join(timeout=SSE_STOP_WAIT_SECS)
        self._sse_thread = None
        print("Stopped")

    @abstractmethod
    def on_orderbook(self, orderbook: OrderBook):
        raise NotImplementedError("You must implement the on_orderbook method!")

    @abstractmethod
    def on_trades(self, trades: list[Trade]):
        raise NotImplementedError("You must implement the on_trades method!")

    def _get_headers(self) -> dict[str, str]:
        return {**STANDARD_HEADERS, "Authorization": self.auth_token}

    def send_order(self, order_request: OrderRequest) -> OrderResponse | None:
        if order_request.volume <= 0:
            raise ValueError("Order volume must be greater than 0")

        payload = {k: v for k, v in asdict(order_request).items() if v is not None}
        response = self._request("POST", "/api/order", json=payload)
        if response is None:
            return None
        if response.status_code == 200:
            order = OrderResponse(**response.json())
            self._orders_sent += 1
            if order.filled and order.filled > 0:
                self._orders_filled += 1
            return order
        else:
            print(f"Failed to send order {order_request}. Response: {response.content}")

    def send_mass_orders(
        self, order_requests: list[OrderRequest]
    ) -> list[OrderResponse | None]:
        for order_request in order_requests:
            if order_request.volume <= 0:
                raise ValueError("Order volume must be greater than 0")

        responses = []

        def worker(order_request, response_list):
            response = self.send_order(order_request)
            response_list.append(response)

        threads = []
        for order_request in order_requests:
            thread = Thread(target=worker, args=(order_request, responses))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        return responses

    def request_all_orders(self) -> list[dict] | None:
        response = self._request("GET", "/api/order/current-user")
        if response is None:
            return None
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to get all orders: {response.content}")

    def cancel_order_by_id(self, order_id: str) -> dict | None:
        response = self._request("DELETE", f"/api/order/{order_id}")
        if response is None:
            return None
        if response.status_code == 200:
            return response.json()

        print(f"Failed to cancel order: {response.content}")

    def cancel_order(self, product: str, price: float) -> dict | None:
        response = self._request(
            "DELETE", "/api/order", params={"product": product, "price": price}
        )
        if response is None:
            return None
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to cancel order: {response.content}")

    def cancel_all_orders(self) -> None:
        for order in self.request_all_orders() or []:
            response = self._request("DELETE", f"/api/order/{order['id']}")
            if response is not None and response.status_code != 200:
                print(f"Failed to cancel order: {response.content}")

    def request_all_products(self) -> list[Product] | None:
        response = self._request("GET", "/api/product")
        if response is None:
            return None
        if response.status_code == 200:
            return list(map(lambda prod: Product(**prod), response.json()))
        else:
            print(f"Failed to get all products: {response.content}")

    def request_positions(self) -> dict[str, int] | None:
        response = self._request("GET", "/api/position/current-user")
        if response is None:
            return None
        if response.status_code == 200:
            return {
                position["product"]: position["volume"] for position in response.json()
            }
        else:
            print(f"Failed to get positions: {response.content}")

    def request_net_positions(self) -> dict[str, int] | None:
        response = self._request("GET", "/api/position/current-user")
        if response is None:
            return None
        if response.status_code == 200:
            return {
                position["product"]: position["net_position"]
                for position in response.json()
            }
        else:
            print(f"Failed to get net positions for the user: {response.content}")

    def _authenticate(self) -> str:
        auth = {"username": self.username, "password": self._password}
        url = f"{self._cmi_url}/api/user/authenticate"
        response = self._http.post(
            url, headers=STANDARD_HEADERS, json=auth, timeout=REQUEST_TIMEOUT_SECS
        )
        response.raise_for_status()

        return response.headers["Authorization"]

    def _register(self) -> None:
        print("Registering the bot")
        response = self._http.post(
            url=f"{self._cmi_url}/api/user",
            json={"username": self.username, "password": self._password},
            headers=STANDARD_HEADERS,
            timeout=REQUEST_TIMEOUT_SECS,
        )
        response.raise_for_status()
        print("Successfully registered")
