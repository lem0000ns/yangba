import json, requests, boto3
from bs4 import BeautifulSoup

s3_client = boto3.client('s3', 'us-west-1')
bucket_name = "nba-players.bucket"

# Function to scrape a website and extract specific elements
def scrape_player_links(url):
    # Send a GET request to the website
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all <td> elements with data-th="Player"
        td_elements = soup.find_all('td', {'data-th': 'Player'})

        # Extract the <a> elements inside the found <td> elements
        player_links = [td.find('a')['href'] for td in td_elements if td.find('a')]

        return player_links
    else:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
        return []


# Function to download an image from a URL
def download_image(url, save_path):
    # Send a GET request to the image URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        #Save in S3
        s3_client.put_object(
            Key=f"playerImages/{save_path}",
            Bucket=bucket_name,
            Body=response.content
        )
    else:
        print(f"Failed to retrieve the image. Status code: {response.status_code}")

def scrape_images(player_links):
    base = 'https://basketball.realgm.com'
    # Send a GET request to the website
    for (i, player_link) in enumerate(player_links):
        response = requests.get(base + player_link)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find the image
            image = soup.findAll('img', {'style': 'border: 1px solid #000; float: left; margin-right: 15px; margin-top:5px;'})
            src = image[0]['src']
            x = src.split('/')[-1].split('_')
            filename = f"{x[1]}-{x[0]}.jpg"  # save under player name

            download_image(base + src, filename)  # or store the image however you want
        else:
            print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
            return []


def lambda_handler(event, context):
    
    url = 'https://basketball.realgm.com/nba/players'
    player_links = scrape_player_links(url)

    print("Finished scraping player links")
    
    # Print the extracted links
    scrape_images(player_links)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

if __name__ == "__main__":
    lambda_handler("", "")