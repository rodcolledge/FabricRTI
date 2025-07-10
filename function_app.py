import azure.functions as func
import logging
from azure.eventhub import EventHubProducerClient, EventData
import yfinance as yf
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Get settings from environment (defined in local.settings.json or app config)
EVENT_HUB_CONN_STR = os.getenv("EVENT_HUB_CONN_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")



@app.route(route="GetStocks")
def GetStocks(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing stock request.')

    tickers_param = req.params.get('name')
    tickers = []

    if tickers_param:
        tickers = [t.strip().upper() for t in tickers_param.split(',') if t.strip()]
    else:
        try:
            req_body = req.get_json()
            tickers_param = req_body.get('name')
            tickers = [t.strip().upper() for t in tickers_param.split(',') if t.strip()]
        except Exception:
            return func.HttpResponse("Please pass a stock ticker (or comma-separated list) as 'name'.", status_code=400)

    if not tickers:
        return func.HttpResponse("No valid tickers provided.", status_code=400)

    results = []

    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONN_STR,
            eventhub_name=EVENT_HUB_NAME
        )
        event_data_batch = producer.create_batch()

        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                info = stock.info or {}
                price = info.get("regularMarketPrice")

                if price is None:
                    raise ValueError("No market price available")

                message = {"ticker": ticker, "price": price}
                event_data_batch.add(EventData(str(message)))
                results.append(f"{ticker} price {price}")
            except Exception as e:
                logging.warning(f"Failed to get data for {ticker}: {e}")
                results.append(f"{ticker} failed: {e}")

        producer.send_batch(event_data_batch)
        producer.close()

        return func.HttpResponse("Pushed to Event Hub: " + "; ".join(results), status_code=200)

    except Exception as e:
        logging.error(f"Event Hub error: {e}")
        return func.HttpResponse(f"Failed: {e}", status_code=500)
