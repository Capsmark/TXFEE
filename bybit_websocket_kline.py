from pybit.unified_trading import WebSocket
from time import sleep

ws = WebSocket(
    testnet=True,
    channel_type="linear",
)


def handle_message(message):
    print(message)


# ws.ticker_stream(
#     symbol="BTCUSDT",
#     callback=handle_message
# )

# ws.orderbook_stream(
#     depth=50,
#     symbol="BTCUSDT",
#     callback=handle_message
# )

ws.trade_stream(
    symbol="BTCUSDT",
    callback=handle_message
)

# ws.kline_stream(
#     interval=5,
#     symbol="BTCUSDT",
#     callback=handle_message
# )

# ws.liquidation_stream(
#     symbol="BTCUSDT",
#     callback=handle_message
# )

# ws.lt_kline_stream(
#     interval=30,
#     symbol="EOS3LUSDT",
#     callback=handle_message
# )

# ws.lt_ticker_stream(
#     symbol="EOS3LUSDT",
#     callback=handle_message
# )

while True:
    sleep(1)
