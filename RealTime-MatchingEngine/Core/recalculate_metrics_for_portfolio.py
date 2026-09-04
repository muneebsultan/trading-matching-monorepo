import requests

class AlertForRecalculatePortfolio():
    def __init__(self):
        pass

    async def alert_response(self, portfolioId):
        try:

            alert_url = f"https://of-traderverse.traderverse.io/api/portfolio-v2/recalculate-metrics-for-portfolio/{portfolioId}"
            # Send GET request
            response = requests.get(alert_url)

            # Check for successful response
            if response.status_code == 200:
                return {"success": f"Success alert: {response.text}"}
            else:
                return {f"Error": f"{response.status_code}: {response.text}"}

        except Exception as e:
            print(f"Error in alert_response: {e}")