import requests

API_URL = "https://cloud-api.yandex.net/v1/disk/public/resources"
def get_img_link(url):
    print(f"processing {url}")
    params = {
        "public_key": url,
        "path": "/"  
    }
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    
    data = response.json()
    direct_link = [x for x in data["sizes"] if x['name'] == 'ORIGINAL'][0]['url']

    return direct_link 

# example 
# example_url = 'https://yadi.sk/i/X5RB2u7EHi6DJQ'
# get_img_link(example_url)
