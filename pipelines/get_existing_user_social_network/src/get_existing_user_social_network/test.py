import time
import random

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from lib.helper import track_performance

# creates an instance of the chrome driver (browser)
driver = webdriver.Chrome()
# hit target site
# driver.get("https://bsky.app/profile/jbouie.bsky.social")

# # Wait for the page to load
# try:
#     # Wait up to 10 seconds for the profile header to be present
#     driver.get("https://bsky.app/profile/jbouie.bsky.social")

#     WebDriverWait(driver, 20).until(
#         EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-testid="profileHeaderDisplayName"]'))
#     )
# except TimeoutException:
#     print("Timed out waiting for page to load")
#     driver.quit()
#     exit(1)

# # Add a small delay to ensure all elements are fully loaded
# driver.implicitly_wait(2)


# # Find the div with the display name
# display_name_div = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="profileHeaderDisplayName"]')
# print("Display Name:", display_name_div.text)

# # Find the anchor with followers information
# followers_anchor = driver.find_element(By.CSS_SELECTOR, 'a[data-testid="profileHeaderFollowersButton"]')
# followers_href = followers_anchor.get_attribute('href')
# followers_text = followers_anchor.text

# # Find the anchor with the follows information
# follows_anchor = driver.find_element(By.CSS_SELECTOR, 'a[data-testid="profileHeaderFollowsButton"]')
# follows_href = follows_anchor.get_attribute('href')
# follows_text = follows_anchor.text

# print("Followers href:", followers_href)
# print("Followers text:", followers_text)
# print("Follows href:", follows_href)
# print("Follows text:", follows_text)

# # Navigate to the followers page
# driver.get(followers_href)

follows_ref = "https://bsky.app/profile/jbouie.bsky.social/follows"

# Navigate to the follows page
driver.get(follows_ref)

# Wait for the follows to load
try:
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href^="/profile/"]'))
    )
except TimeoutException:
    print("Timed out waiting for follows to load")
    driver.quit()
    exit(1)


# Function to scroll and get all follows
@track_performance
def get_all_follows():
    follows_data = set()
    existing_hrefs = set()
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        # Find all anchor elements with href starting with "/profile/"
        follow_anchors = driver.find_elements(By.CSS_SELECTOR, 'a[href^="/profile/"]')

        # Extract aria-label and href for each follow
        for anchor in follow_anchors:
            aria_label = anchor.get_attribute("aria-label")
            href = anchor.get_attribute("href")
            if aria_label and href:
                # Check if aria_label doesn't have any spaces.
                # There are 2 anchors, one with the actual handle and one that
                # has only the user's profile picture. The profile picture will
                # have an aria-label of "[Name]'s avatar", so we want to filter
                # these out.
                if " " not in aria_label and href not in existing_hrefs:
                    follows_data.add((aria_label, href))
                    existing_hrefs.add(href)
                    if len(follows_data) % 10 == 0:
                        print(f"Added {len(follows_data)} follows so far...")
                else:
                    pass

        # Scroll down slowly
        current_height = driver.execute_script("return window.pageYOffset")
        target_height = driver.execute_script("return document.body.scrollHeight")
        step = 30  # Adjust this value to control scroll speed (smaller value = slower scroll)
        while current_height < target_height:
            current_height += step
            driver.execute_script(f"window.scrollTo(0, {current_height});")
            time.sleep(
                0.1 + random.uniform(0, 0.25)
            )  # Add a small delay with slight randomness between each scroll step

        # Wait for the page to load
        time.sleep(5)

        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            # Progressive backoff to check if more content needs to load
            for wait_time in [10, 20, 30, 30]:
                time.sleep(wait_time)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height > last_height:
                    last_height = new_height
                    break
            else:  # matches with the for-loop, if we've gone through all wait times and still no change
                # If we've gone through all wait times and still no change
                print("Rechecking the entire page one last time...")
                follow_anchors = driver.find_elements(
                    By.CSS_SELECTOR, 'a[href^="/profile/"]'
                )
                for anchor in follow_anchors:
                    aria_label = anchor.get_attribute("aria-label")
                    href = anchor.get_attribute("href")
                    if aria_label and href:
                        if " " not in aria_label and href not in existing_hrefs:
                            follows_data.add((aria_label, href))
                            existing_hrefs.add(href)
                            if len(follows_data) % 10 == 0:
                                print(f"Added {len(follows_data)} follows so far...")
                        else:
                            print(
                                f"Skipping entry ({aria_label}, {href}) (either spaces or it already exists...)"
                            )
                print("Final recheck complete. No more follows to load.")
                break

        last_height = new_height

        # Re-check the entire page for new elements
        follow_anchors = driver.find_elements(By.CSS_SELECTOR, 'a[href^="/profile/"]')
        for anchor in follow_anchors:
            aria_label = anchor.get_attribute("aria-label")
            href = anchor.get_attribute("href")
            if aria_label and href:
                if " " not in aria_label and href not in existing_hrefs:
                    follows_data.add((aria_label, href))
                    existing_hrefs.add(href)
                    if len(follows_data) % 10 == 0:
                        print(f"Added {len(follows_data)} follows so far...")
                else:
                    print(
                        f"Skipping entry ({aria_label}, {href}) (either spaces or it already exists...)"
                    )

    return list(follows_data)


# Get all follows
follows_data = get_all_follows()

# Print the total number of results
print(f"Total follows found: {len(follows_data)}")

breakpoint()

# kill browser instance
driver.quit()
