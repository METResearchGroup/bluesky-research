# from seleniumwire import webdriver  # Import from seleniumwire
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# import json

# # Set up headless Chrome
# chrome_options = Options()
# chrome_options.add_argument("--headless")
# chrome_options.add_argument("--disable-gpu")
# chrome_options.add_argument("--no-sandbox")

# # Path to your chromedriver
# service = Service("/usr/local/bin/chromedriver-mac-arm64/chromedriver")

# # Initialize the WebDriver
# driver = webdriver.Chrome(service=service, options=chrome_options)

# # Navigate to the URL
# driver.get("https://bsky.app/profile/bsky.app/follows")

# # Intercept the network request
# for request in driver.requests:
#     if request.response and "app.bsky.graph.getFollows" in request.url:
#         response_body = request.response.body
#         data = json.loads(response_body)

#         # Extract "did" and "handle" from the response
#         follows = data.get("follows", [])
#         for follow in follows:
#             did = follow.get("did")
#             handle = follow.get("handle")
#             print(f"DID: {did}, Handle: {handle}")
#         break

# # Close the WebDriver
# driver.quit()

# if __name__ == "__main__":
#     print("Foo!")
#     breakpoint()
